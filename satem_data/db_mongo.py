import pymongo
from pymongo import MongoClient
from decouple import config


def get_database(db_name):

    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    url = config("CREA_MONGODB_URL", None)
    if not url:
        raise EnvironmentError("Missing CREA_MONGODB_URL environment variable.")

    client = MongoClient(url)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client[db_name]


def get_collection(collection_name, db=None, db_name=None):
    if db is None:
        db = get_database(db_name=db_name)

    return db[collection_name]


def create_index(collection, cols, unique=False):
    collection.create_index(
        [(x, pymongo.ASCENDING) for x in cols],
        unique=unique
    )