#!/usr/bin/env python3

from pymongo.collection import Collection
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from pymongo import ReadPreference
import logging
from copy import deepcopy
from datetime import datetime, timezone

log = logging.getLogger(__name__)


class Change:
    (INITIAL, ADD, REMOVE, UPDATE) = list("IARU")


class PatchResult(list):
    def __init__(self, *patches):
        super().__init__()

    def __str__(self):
        return "<PatchResult (patches=[{}])>".format(", ".join([str(i) for i in self]))


class HistoricalCollection(Collection):
    """Record everything associated with a collection."""

    DEFAULT_num_deltas_before_snapshot = 5
    DEFAULT_internal_metadata_keyname = '__HISTORICAL_COLLECTION_INTERNAL_METADATA'

    def __new__(cls, *args, **kwargs):
        """Mainly checks to ensure all subclasses have a PK_FIELDS attribute."""
        if not hasattr(cls, "PK_FIELDS"):
            raise AttributeError("{} is missing PK_FIELDS".format(cls.__name__))
        return super().__new__(cls)

    @property
    def _deltas_name(self):
        return "{}_deltas".format(self.name)

    @property
    def _deltas_collection(self):
        """Shortcut property to access the `deltas` collection."""
        return self.database[self._deltas_name]

    def __init__(self, *args, **kwargs):
        """
        Construct a new HistoricalCollection.

        :param db: Mongo database instance

        :param name: (Optional) If not given, will default to the class name.

        :param internal_metadata_keyname: (Optional) Internal name of key to use in the doc
                                          to keep track of deltas. Defaults to 
                                          DEFAULT_internal_metadata_keyname

        :param num_deltas_before_snapshot: (Optional) How many deltas do you want before creating
                                           a snapshot?  If your data changes often you probably 
                                           want a higher number here.  Defaults to 
                                           DEFAULT_num_deltas_before_snapshot. Also the more 
                                           snapshots you have the bigger the collection will be.
        """

        if "name" not in kwargs:
            kwargs["name"] = type(self).__name__

        self.num_deltas_before_snapshot = kwargs.pop('num_deltas_before_snapshot', HistoricalCollection.DEFAULT_num_deltas_before_snapshot)
        self.internal_metadata_keyname = kwargs.pop('internal_metadata_keyname', HistoricalCollection.DEFAULT_internal_metadata_keyname)

        self.timestamp = datetime.now(timezone.utc)

        super().__init__(*args, **kwargs)

    def _document_filter(self, document):
        """Create a document filter based on the class's PK_FIELDS."""
        try:
            return dict([(k, document[k]) for k in self.PK_FIELDS])
        except KeyError as e:
            if bool(set(e.args) & set(self.PK_FIELDS)):
                raise KeyError(
                    "Perhaps you forgot to include {} in projection?".format(
                        self.PK_FIELDS
                    )
                )

    def revisions(self, *args, **kwargs):
        # TODO Update to get a list of revision versions... and maybe metadata?
        return ""

    def _check_key(self, *docs):
        """Verify that the same `PK_FIELDS` field is present for every doc."""
        pks = set()
        for pk in self.PK_FIELDS:
            for (i, d) in enumerate(docs):
                try:
                    pks.add(d[pk])
                except KeyError as e:
                    raise AttributeError("Keys not present: {}".format(pk))
        if len(list(pks)) > 1:
            raise AttributeError("Differing keys present: {}".format(list(pks)))

    def _get_additions(self, latest, doc, ignore_fields=None):
        self._check_key(latest, doc)
        latest_keyset = set(latest.keys())
        doc_keyset = set(doc.keys())
        if not ignore_fields:
            ignore_fields = []

        ignore_fields += ['_id', self.internal_metadata_keyname]

        for field in ignore_fields:
            latest_keyset.discard(field)
            doc_keyset.discard(field)

        return dict([(k, doc[k]) for k in doc_keyset - latest_keyset])

    def _get_updates(self, latest, doc, ignore_fields=None):
        if not ignore_fields:
            ignore_fields = []

        ignore_fields += [self.internal_metadata_keyname]
        return dict(
            [(k, v) for (k, v) in doc.items() if k in latest and latest[k] != doc[k] and k not in ignore_fields]
        )

    def _get_removals(self, latest, doc, ignore_fields=None):
        self._check_key(latest, doc)
        # This will get all keys that are NOT latest, but are in doc.
        # We'll be skipping '_id', since that's an internal MongoDB key.
        if not ignore_fields:
            ignore_fields = []

        ignore_fields += ["_id", self.internal_metadata_keyname]
        return [
            x
            for x in list(set(latest.keys()) - set(doc.keys()))
            if x not in ignore_fields
        ]

    def _add_patch(self, patch):
        return self._deltas_collection.insert_one(patch)

    def _create_deltas(self, last, current, ignore_fields=None):
        return {
            Change.ADD: self._get_additions(last, current, ignore_fields),
            Change.UPDATE: self._get_updates(last, current, ignore_fields),
            Change.REMOVE: self._get_removals(last, current, ignore_fields),
        }

    def delete_doc_and_patches(self, *args, **kwargs):
        # TODO Update delete by delta instead of filter
        doc = args[0]
        fltr = self._document_filter(doc)
        log.debug("Deleting %s", doc)
        if super().delete_one(*args, **kwargs):
            # Delete all patches
            self._deltas_collection.delete_many(fltr)

    def get_revision_by_date(self, doc, version_timestamp):
        """
        Get the document as it existed at a point in time

        :param doc: Live document that we want to find the previous version for

        :param version_timestamp: The point in time that for which we are attempting to get the version.
        """
        doc_revision = None

        # if the doc was created after the timestamp then we know it wasn't around back then.
        if doc[self.internal_metadata_keyname]['created']['timestamp'] > version_timestamp:
            return None

        deltas = []
        base_doc = deepcopy(super().find_one({'_id': doc['_id']}))

        # Get all the revisions from now until one before version_timestamp
        delta_id = base_doc[self.internal_metadata_keyname]['previous_delta']

        while True:
            delta = self._deltas_collection.find_one({'_id': delta_id})
            if delta is not None and 'timestamp' in delta[self.internal_metadata_keyname]:
                if delta[self.internal_metadata_keyname]['timestamp'] < version_timestamp:
                    # This is our last one.
                    if delta[self.internal_metadata_keyname]['type'] == 'snapshot':
                        doc_revision = deepcopy(delta)
                    else:
                        deltas.append(deepcopy(delta))

                    break
                elif delta[self.internal_metadata_keyname]['type'] == 'snapshot':
                    deltas.clear()
                    base_doc = deepcopy(delta)
                elif 'deltas' in delta[self.internal_metadata_keyname]:
                    deltas.append(delta[self.internal_metadata_keyname]['deltas'])
                else:
                    # error
                    print("ERROR ERROR")
                    break

                if 'previous_delta' in delta[self.internal_metadata_keyname]:
                    delta_id = delta[self.internal_metadata_keyname]['previous_delta']
                else:
                    # We're at the beginning.  Assume that delta[self.internal_metadata_keyname]['type'] == 'snapshot' caught it?
                    doc_revision = base_doc
                    break

                #if delta[self.internal_metadata_keyname]['timestamp'] < version_timestamp or \
                #        'previous_delta' not in delta[self.internal_metadata_keyname] or \
                #        delta[self.internal_metadata_keyname]['previous_delta'] is None:
                #    break
                #else:
                #    delta_id = delta[self.internal_metadata_keyname]['previous_delta']
            else:
                # Reached the end and no deltas
                break

        if doc_revision is None:
            doc_revision = self._apply_patches(base_doc, deltas)
            
        if self.internal_metadata_keyname in doc_revision:
            del doc_revision[self.internal_metadata_keyname]

        return doc_revision


    def _apply_patches(self, doc, deltas):
        """
        Takes a list of deltas and applies them in order to the starting doc

        :param doc: The starting/latest doc
        :param deltas: a list of deltas
        """
        for delta in deltas:
            for (k, v) in delta.get(Change.ADD, {}).items():
                doc[k] = v
                for (k, v) in delta.get(Change.UPDATE, {}).items():
                    doc[k] = v
                for k in delta.get(Change.REMOVE, []):
                    if k not in doc:
                        log.warning("'%s' wasn't in instance %s. This was unexpected, so skipping.", k, doc)
                    else:
                        del doc[k]

        return doc


    def get_revision_by_version(self, version_major, version_minor):
        doc = None

        # Get the first one
        starting_revision = self._deltas_collection.find_one({
            f"{self.internal_metadata_keyname}.version.major": version_major,
            f"{self.internal_metadata_keyname}.version.minor": version_minor
        })
        if starting_revision:
            if starting_revision[self.internal_metadata_keyname]['type'] == 'snapshot':
                # We are at the snapshot.  There is nothing to do
                starting_revision.discard('_id')
                metadata = starting_revision[self.internal_metadata_keyname]
                starting_revision.discard(self.internal_metadata_keyname)
                # Add back in the metadata and version
                starting_revision[self.internal_metadata_keyname] = {'version': metadata['version'], 'metadata': metadata['metadata']}
                doc = deepcopy(starting_revision)
            else:
                # 1) Find the next snapshot AFTER the version
                # 2) walk back to the version
                after_snapshot = None
                deltas = [starting_revision]
                delta_id = starting_revision['_id']
                while True:
                    revision = self._deltas_collection.find_one({f"{self.internal_metadata_keyname}.previous_delta": delta_id})
                    if revision:
                        if revision[self.internal_metadata_keyname]['type'] == 'snapshot':
                            # Found it!  We can stop now.
                            after_snapshot = revision
                            break
                        else:
                            # Just add it to deltas and grab the next one
                            deltas.append(revision)
                            delta_id = revision['_id']
                    else:
                        break

                if not after_snapshot:
                    # If we are here that means there are no "after" snapshots.  Lets use the live version.
                    after_snapshot = super().find_one({f"{self.internal_metadata_keyname}.previous_delta": deltas[-1]['_id']})

                if after_snapshot:
                    doc = deepcopy(after_snapshot)
                    deltas.reverse()
                    for delta in deltas:
                        for (k, v) in delta['deltas'].get(Change.ADD, {}).items():
                            doc[k] = v
                        for (k, v) in delta['deltas'].get(Change.UPDATE, {}).items():
                            doc[k] = v
                        for k in delta['deltas'].get(Change.REMOVE, []):
                            if k not in doc:
                                log.warning("'%s' wasn't in instance %s. This was unexpected, so skipping.", k, doc)
                            else:
                                del doc[k]

                        # Lets include version and metadata in there
                        doc[self.internal_metadata_keyname]['version'] = delta[self.internal_metadata_keyname]['version']
                        doc[self.internal_metadata_keyname]['metadata'] = delta[self.internal_metadata_keyname]['metadata']

                    del doc[self.internal_metadata_keyname]['previous_delta']

        return doc

    def _do_patch_callback(self, session, doc, metadata, force, ignore_fields, **kwargs):
        """
        Transaction callback for comparing the doc with the latest version and updating/patching if necessary.

        :param session: MongoClient session object.

        :param doc: doc object to compare with the latest version in the collection.

        :param metadata: (Optional) Defaults to None. dict of attributes to attach to the doc record in its internal
                         metadata field.

        :param force: (Optional) Defatuls to False. Normally we won't patch or update anything unless there is a
                      diff between the latest doc in the collection and the doc that was passed in here, but
                      this flag will force a (empty) patch being added to the deltas collection.

        :param ignore_fields: (Optional) list of fields to ignore when comparing the latest doc in the collection
                              with the doc that was passed in here.

        """
        # This seems like a lot to do in a single transaction... the db will be locked during this time?

        fltr = self._document_filter(doc)
        latest = super().find_one(fltr, **kwargs)

        patch_result = None

        if latest is None or (not len(latest)) or self.internal_metadata_keyname not in latest:
            # No doc with PK_FIELDS, so let's add one and create an initial snapshot

            # Create and add the patch
            doc_patch = deepcopy(doc)
            doc_patch[self.internal_metadata_keyname] = {
                'type': 'snapshot',
                'version': {
                    'major': 0,
                    'minor': 0
                }, 
                'timestamp': self.timestamp,
                'metadata': None
            }
            new_patch_result = self._add_patch(doc_patch)

            if new_patch_result:
                # if the patch is successful, lets add the real doc
                doc[self.internal_metadata_keyname] = {
                        'previous_delta': new_patch_result.inserted_id,
                        'version': {
                            'major': 1, 
                            'minor': 0
                        }, 
                        'deleted': None,
                        'created': {'timestamp': self.timestamp, 'metadata': metadata},
                        'updated': {'timestamp': self.timestamp, 'metadata': metadata},
                        }
                result = super().insert_one(doc, **kwargs)
                patch_result = PatchResult(result)
        else:
            # An exising doc exists.  Lets create a patch and then determine if we need to update it.
            deltas = self._create_deltas(doc, latest, ignore_fields)

            if deltas['A'] or deltas['R'] or deltas['U'] or force:
                # there was a delta or the force flag was passed.  Let's add a patch to the collection then update the live doc
                # If we are self.num_deltas_before_snapshot due, dump the whole doc as the patch. otherwise patch as normal
                delta_id = latest[self.internal_metadata_keyname]['previous_delta']

                i = 1
                while i < self.num_deltas_before_snapshot:
                    # Look for a snapshot less than self.num_deltas_before_snapshot deltas back starting from the previous_delta of live doc
                    delta = self._deltas_collection.find_one({'_id': delta_id})
                    if delta:
                        if delta[self.internal_metadata_keyname]['type'] == 'snapshot':
                            # We found a recent snapshot.  We're good.
                            break
                        else:
                            delta_id = delta[self.internal_metadata_keyname]['previous_delta']
                            if not delta_id:
                                # error
                                break
                    else:
                        break

                    i += 1

                if i == self.num_deltas_before_snapshot:
                    # We made it all the way before finding a snapshot and we are due.  Let's create one now.
                    doc_patch = deepcopy(doc)
                    doc_patch[self.internal_metadata_keyname] = {
                        'previous_delta': latest[self.internal_metadata_keyname]['previous_delta'],
                        'type': 'snapshot',
                        'version': latest[self.internal_metadata_keyname]['version'],  # snag the live version
                        'timestamp': self.timestamp,
                        'metadata': latest[self.internal_metadata_keyname]['metadata']
                    }
                    new_patch_result = self._add_patch(doc_patch)

                    if new_patch_result:
                        doc[self.internal_metadata_keyname] = {
                            'previous_delta': new_patch_result.inserted_id,
                            'version': {
                                'major': latest[self.internal_metadata_keyname]['version']['major'] + 1,
                                'minor': 0
                            },
                            'deleted': None,
                            'created': latest[self.internal_metadata_keyname].get('created', {'timestamp': self.timestamp, 'metadata': None}),
                            'updated': {'timestamp': self.timestamp, 'metadata': metadata}
                        }
                        result = super().replace_one({'_id': latest['_id']}, doc)
                        patch_result = PatchResult(result)
                else:
                    # Create the patch like normal
                    patch = {
                        self.internal_metadata_keyname: {
                            'previous_delta': latest[self.internal_metadata_keyname]['previous_delta'],
                            'type': 'patch',
                            'deltas': deltas,
                            'version': latest[self.internal_metadata_keyname]['version'],
                            'timestamp': self.timestamp,
                            'metadata': latest[self.internal_metadata_keyname]['updated']['metadata'],
                        }
                    }
                    result = self._add_patch(patch)

                    if result:
                        doc[self.internal_metadata_keyname] = {
                            'previous_delta': result.inserted_id,
                            'version': {
                                'major': latest[self.internal_metadata_keyname]['version']['major'],
                                'minor': latest[self.internal_metadata_keyname]['version']['minor'] + 1
                            },
                            'deleted': None,
                            'created': latest[self.internal_metadata_keyname].get('created', {'timestamp': self.timestamp, 'metadata': None}),
                            'updated': {'timestamp': self.timestamp, 'metadata': metadata}
                        }
                        patch_result = PatchResult(super().replace_one({'_id': latest['_id']}, doc))

        return patch_result

    def patch_one(self, *args, **kwargs):
        """
        Patch one document.

        :param doc: Document object to patch.

        :param force: (Optional) Defatuls to False. Normally we won't patch or update anything unless there is a
                      diff between the latest doc in the collection and the doc that was passed in here, but
                      this flag will force a (empty) patch being added to the deltas collection.

        :param ignore_fields: (Optional) list of fields to ignore when comparing the latest doc in the collection
                              with the doc that was passed in here.

        :param metadata: (Optional) Defaults to None. dict of attributes to attach to the doc record in its internal
                         metadata field.

        """
        doc = args[0]
        force = kwargs.pop("force", False)
        ignore_fields = kwargs.pop("ignore_fields", None)
        metadata = kwargs.pop("metadata", None)

        result = None
        with self.database.client.start_session() as session:
            result = session.with_transaction(
                lambda session: self._do_patch_callback(session, doc, metadata,
                                                        force, ignore_fields,
                                                        **kwargs),
                read_concern=ReadConcern("local"),
                write_concern=WriteConcern("majority", wtimeout=1000),
                read_preference=ReadPreference.PRIMARY,
            )

        return result

    def patch_many(self, docs, *args, **kwargs):
        """
        Patch an array of docs

        :param docs: A list of document objects to patch

        :param missing_mark_deleted: (Optional) Defaults to False. This function can optionally scan the collection
                                     for any documents NOT passed in through docs and mark them as deleted.

        :param missing_mark_deleted_filter: (Optional) filter object that will be "and"ed to the list
                                            of documents to product the list of documents to mark
                                            deleted.  For example, if your collection is a set of contacts
                                            for different clients, and you pass in a list representing all the
                                            current contacts for that client, you can also pass in a filter here
                                            so we know to only mark conacts deleted if they are related to the
                                            client AND not in the list of docs passed in.

        :param metadata: (Optional) Defaults to None. dict of attributes to attach to the doc record in its internal
                         metadata field.


        """
        missing_mark_deleted = kwargs.pop("missing_mark_deleted", False)
        missing_mark_deleted_filter = kwargs.pop("missing_mark_deleted_filter", {})
        metadata = kwargs.get("metadata", {})

        result = []
        for doc in docs:
            one = self.patch_one(doc, *args, **kwargs)
            if one:
                result.append(one)

        if missing_mark_deleted:
            # Get a list of docs in the collection that are not already marked as deleted
            # first get a list of PKs from passed-in docs
            db_filter = {}
            for pk in self.PK_FIELDS:
                db_filter.update({pk: 1})

            existing_pks = {}
            for pk in self.PK_FIELDS:
                existing_pks.update({ pk: [] })
                for row in docs:
                    existing_pks[pk].append(row[pk])

            # Then lookup in this collection anything thats not in the list
            db_filter = []
            for pk in self.PK_FIELDS:
                db_filter.append({ pk: {'$not': { '$in': existing_pks[pk] } }})

            db_filter.append({f"{self.internal_metadata_keyname}.deleted.timestamp": None})

            missing_docs = super().find({ '$and': db_filter })

            missing_ids = []
            for md in missing_docs:
                missing_ids.append(md['_id'])

            # For all those, update the metadata to be deleted
            if missing_ids:
                if missing_mark_deleted_filter:
                    db_filter = { '$and': [ missing_mark_deleted_filter, {'_id': {'$in': missing_ids}} ]}
                else:
                    db_filter = {'_id': {'$in': missing_ids}}

                result.append(super().update_many(db_filter, {'$set': {f"{self.internal_metadata_keyname}.deleted": {'timestamp': self.timestamp, 'metadata': metadata}}}))

        return result
