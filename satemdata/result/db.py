import json
from collections import OrderedDict

import db_mongo

from . import DB_SATEM
from . import RESULTS_COLLECTION
from . import RESULTS_UNIQUE_COLS


def get_result_db(db_name=DB_SATEM):
    return db_mongo.get_database(db_name=db_name)


def get_result_col(db=None, db_name=DB_SATEM, collection_name=RESULTS_COLLECTION):
    return db_mongo.get_collection(collection_name=collection_name,
                                   db=db,
                                   db_name=db_name)


def create_result_index(result_col=get_result_col()):
    return db_mongo.create_index(collection=result_col,
                    cols=RESULTS_UNIQUE_COLS,
                    unique=True)


def enforce_schema(db=get_result_db()):
    with open('satemdata/result/schemas/result.json', 'r') as j:
        schema = json.loads(j.read())

    schema = OrderedDict(schema)
    return db.command(schema)