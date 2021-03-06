import json
from collections import OrderedDict

from satemdata import db_mongo

from . import DB_SATEM
from . import FEATURES_COLLECTION
from . import FEATURES_UNIQUE_COLS


def get_feature_db(db_name=DB_SATEM):
    return db_mongo.get_database(db_name=db_name)


def get_feature_col(db=None, db_name=DB_SATEM, collection_name=FEATURES_COLLECTION):
    return db_mongo.get_collection(collection_name=collection_name,
                                   db=db,
                                   db_name=db_name)


def create_feature_index(feature_col=None):
    if feature_col is None:
        feature_col = get_feature_col()

    return db_mongo.create_index(collection=feature_col,
                    cols=FEATURES_UNIQUE_COLS,
                    unique=True)


def enforce_schema(db=None):
    if db is None:
        db = get_feature_db()

    with open('satemdata/feature/schemas/feature.json', 'r') as j:
        schema = json.loads(j.read())

    schema = OrderedDict(schema)
    db.command(schema)