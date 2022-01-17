from pymongo import MongoClient
import pymongo
from decouple import config

from . import FEATURES_UNIQUE_COLS
from . import FEATURES_COLLECTION


def get_database():

    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    url = config("CREA_MONGODB_URL", None)
    if not url:
        raise EnvironmentError("Missing CREA_MONGODB_URL environment variable.")

    db_name = "satem"
    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    from pymongo import MongoClient
    client = MongoClient(url)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client[db_name]


def get_feature_collection(db=None):

    if not db:
        db = get_database()

    return db[FEATURES_COLLECTION]


def create_feature_index(feature_col):
    feature_col.create_index(
        [(x, pymongo.ASCENDING) for x in FEATURES_UNIQUE_COLS],
        unique=True
    )