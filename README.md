# historical-collection-ng
Library to manage version history for MongoDB documents

This project started as simple tweaks to Jordan Hewitt's historical_collection found [here](https://gitlab.com/srcrr/historical_collection) but at this point almost all of the code is new. Go check out the original and see if that works for you also.

Here are some notable features of this library. Many of these are different from the original:
- Version'ed documents. All the deltas are stored in a separate deltas table, called {CollectionName}_deltas by default.  The library manages this collection.
- The version of the doc in the original {CollectionName} collection is always the latest version. This makes it very easy to query the live version since in my applications, that is the most-referenced version.
- To help with speed when replaying the deltas (e.g. when you're getting a previous revision), the library also supports automatic snapshots. This functionality will snapshot the live document in the _deltas collection every X deltas (5 by default).
- The library will only update the deltas or live version if any the document attributes (except for those in the ``ignore_fields`` parameter) do not match.. unless you pass in ``force=True``.
- When you update or insert a document into the collection, this library will add a key ("__HISTORICAL_COLLECTION_INTERNAL_METADATA") to the original document to keep track of the created, deleted, and updated states, the first snapshot reference, and to hold arbitrary metadata that you may want to pass in.
- The original version relies on non-guaranteed consistency when finding all the deltas to give you the "live" version.  This version keeps an explicit delta chain instead.
- We attempt to query and make changes in a transaction so that our reads and write are autonomous and consistency is guaranteed.


## To use this library:

First, set up your imports and create a class that includes a PK_FIELDS variable to include the primary keys of the collection. PK_FIELDS is used to tell if the passed-in doc has a version already in the live collection and should include whatever field(s) you need to determine that. If a live version already exists, the library will perform the deltas work. If not, the library will create a new live document.

    from historical_collection import HistoricalCollection
    from pymongo import MongoClient
    class Contacts(HistoricalCollection):
        PK_FIELDS = ['email', ]  # <<= This is the only requirement

After you have your class and PK_FIELDS set up, connect to the database (python):

    db_host = "localhost"
    db_port = 27017
    db_user = "dbuser"
    db_pass = "password123"
    db_name = "FancyProjectDatabase"
    app_name = "FancyProject"

    coll_contacts_collection_name = "Contacts"
    
    client = MongoClient(host=[f"{db_host}:{db_port}"], username=db_user, password=db_pass, authSource=db_name, appname=app_name, authMechanism='SCRAM-SHA-256')
    db = client[db_name]
    
    coll_Contacts = Contacts(database=db, name=coll_contacts_collection_name)

The default internal metadata keyname and the number of deltas before snapshots are configurable in the library or when you instantiate your collection class (python):

    coll_contacts = Contacts(database=db, name=coll_contacts_collection_name, internal_metadata_keyname="__ABCD", num_deltas_before_snapshot=20)


Now that your collection class is set up and you're connected to the database, you use 2 main functions to interact with the library:

``object.patch_one(doc, force, ignore_fields, metadata)`` 

    :param doc: Document object to patch.

    :param force: (Optional) Defatuls to False. Normally we won't patch or update anything unless there is a
                  diff between the latest doc in the collection and the doc that was passed in here, but
                  this flag will force a (empty) patch being added to the deltas collection.

    :param ignore_fields: (Optional) list of fields to ignore when comparing the latest doc in the collection
                          with the doc that was passed in here.

    :param metadata: (Optional) Defaults to None. dict of attributes to attach to the doc record in its internal
                     metadata field.

and

``patch_many(docs, missing_mark_deleted, missing_mark_deleted_filter, force, ignore_fields, metadata)``

    :param docs: A list of document objects to patch

    :param missing_mark_deleted: (Optional) Defaults to False. This function can optionally scan the collection
                                 for any documents NOT passed in through docs and mark them ad deleted.

    :param missing_mark_deleted_filter: (Optional) filter object that will be "and"ed to the list
                                        of documents to product the list of documents to mark
                                        deleted.  For example, if your collection is a set of contacts
                                        for different clients, and you pass in a list representing all the
                                        current contacts for that client, you can also pass in a filter here
                                        so we know to only mark conacts deleted if they are related to the
                                        client AND not in the list of docs passed in.

    :param metadata: (Optional) Defaults to None. dict of attributes to attach to the doc record in its internal
                     metadata field.


Examples (python):

    coll_contacts.patch_one({"DisplayName": "Joe Bagadonuts", "email": "joe@donutco.com", "FavoriteRestaurant": "McDonalds", "RecordOwner": "Patrick"})
    coll_contacts.patch_one({"DisplayName": "Jane Smith", "email": "jane@gmail.com", "FavoriteFood": "Chicken fingers", "RecordOwner": "John"})
    coll_contacts.patch_one({"DisplayName": "Daniel Twilco", "email": "jazz@rabbit.com", "Address1": "123 In a hole St", "RecordOwner": "Patrick"}, metadata={"sync_source": "My CRM Software"}, force=True)
    coll_contacts.patch_one({"DisplayName": "Rodney DangerMouse", "email": "danger@middlename.com", "Birthday": "1967-04-05", "RecordOwner": "John"}, force=True)

or

    contacts = [
        {"DisplayName": "Joe Bagadonuts", "email": "joe@donutco.com", "FavoriteRestaurant": "McDonalds", "RecordOwner": "Patrick"},
        {"DisplayName": "Jane Smith", "email": "jane@gmail.com", "FavoriteFood": "Chicken fingers", "RecordOwner": "Patrick"},
        {"DisplayName": "Daniel Twilco", "email": "jazz@rabbit.com", "Address1": "123 In a hole St", "RecordOwner": "Patrick"},
        {"DisplayName": "Rodney DangerMouse", "email": "danger@middlename.com", "Birthday": "1967-04-05", "RecordOwner": "Patrick"}
    ]
    coll_contacts.patch_many(contacts, missing_mark_deleted=True, missing_mark_deleted_filter={"Owner": "Patrick"}, force=True, metadata={"sync_source": "My CRM Software"})

The documents look like this in the collection.  Note this is from the first ``patch_one`` example above (mongosh):

    FancyProjectDatabase> db.Contacts.findOne({"email": "joe@donutco.com"})
    {
      _id: ObjectId("653e6f50c42acb44d4ecf994"),
      DisplayName: 'Joe Bagadonuts',
      email: 'joe@donutco.com',
      FavoriteRestaurant: 'McDonalds',
      RecordOwner: 'Patrick',
      __HISTORICAL_COLLECTION_INTERNAL_METADATA: {
          previous_delta: ObjectId("953e6a68c42acbd4d4ecf993"),
          version: { major: 1, minor: 0 },
          deleted: {},
          created: {
              timestamp: ISODate("2023-09-29T14:19:32.863Z"),
              metadata: null
          },
          updated: {
              timestamp: ISODate("2023-09-29T14:19:32.863Z"),
              metadata: null
          }
    }

And the initial delta looks like this (mongosh):

    FancyProjectDatabase> db.Contacts_deltas.findOne({"_id": ObjectId("953e6a68c42acbd4d4ecf993")})
    {
      _id: ObjectId("953e6a68c42acbd4d4ecf993"),
      DisplayName: 'Joe Bagadonuts',
      email: 'joe@donutco.com',
      FavoriteRestaurant: 'McDonalds',
      __HISTORICAL_COLLECTION_INTERNAL_METADATA: {
          type: 'snapshot',
          version: { major: 0, minor: 0 },
          timestamp: ISODate("2023-09-29T14:19:32.863Z")
          metadata: null
    }

Now lets update it (python):

    coll_contacts.patch_one({"DisplayName": "Joe Bagadonuts", "email": "joe@donutco.com", "FavoriteRestaurant": "Burger King", "Car": "2022 BMW X5", "RecordOwner": "Patrick"})

Now the live document looks like this(mongosh):

    FancyProjectDatabase> db.Contacts.findOne({"email": "joe@donutco.com"})
    {
      _id: ObjectId("653e6f50c42acb44d4ecf994"),
      DisplayName: 'Joe Bagadonuts',
      email: 'joe@donutco.com',
      FavoriteRestaurant: 'Burger King',
      Car: '2022 BMW X5',
      RecordOwner: 'Patrick',
      __HISTORICAL_COLLECTION_INTERNAL_METADATA: {
          previous_delta: ObjectId("653e6a68c42acb44d4ecf9f1"),
          version: { major: 1, minor: 1 },
          deleted: {},
          created: {
              timestamp: ISODate("2023-09-29T14:19:32.863Z"),
              metadata: null
          },
          updated: {
              timestamp: ISODate("2023-09-30T11:46:14.791Z"),
              metadata: null
          }
    }

And the latest delta looks like this (mongosh):

    FancyProjectDatabase> db.Contacts_deltas.findOne({"_id": ObjectId("653e6a68c42acb44d4ecf9f1")})
    {
      _id: ObjectId("653e6a68c42acb44d4ecf9f1"),
      __HISTORICAL_COLLECTION_INTERNAL_METADATA: {
          previous_delta: ObjectId("953e6a68c42acbd4d4ecf993")
          deltas: { A: { }, U: { FavoriteRestaurant: 'McDonalds' }, R: ['Car'] },
          type: 'patch',
          version: { major: 1, minor: 0 },
          timestamp: ISODate("2023-09-29T14:19:32.863Z")
          metadata: null
    }
