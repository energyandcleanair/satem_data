import datetime as dt
import pytest
import json
from pymongo.errors import DuplicateKeyError, WriteError, BulkWriteError


import feature
from feature import db
from feature.crud import *
from feature import FEATURES_UNIQUE_COLS
from feature import FEATURES_COLLECTION
from feature import DB_SATEM_TEST
from feature import DATE_FORMAT


@pytest.fixture
def db_test():
    return db.get_feature_db(DB_SATEM_TEST)


@pytest.fixture
def feature_col():
    feature_db = db.get_feature_db(DB_SATEM_TEST)
    feature_db.drop_collection(FEATURES_COLLECTION)
    feature_col = db.get_feature_col(db=feature_db)
    return feature_col


@pytest.fixture
def features():
    return [
        {
            'location_id': 'test001',
            'date': dt.date.today().strftime(DATE_FORMAT),
            'tropomi_no2': {}
        },{
            'location_id': 'test001',
            'date': (dt.date.today()-dt.timedelta(days=1)).strftime(DATE_FORMAT),
            'tropomi_no2': {}
        },{
            'location_id': 'test002',
            'date': dt.date.today().strftime(DATE_FORMAT),
            'tropomi_no2': {}
        },{
            'location_id': 'test002',
            'date': (dt.date.today()-dt.timedelta(days=1)).strftime(DATE_FORMAT),
            'tropomi_no2': {}
        }]


def test_crud_feature(feature_col, features):

    item = features[0]
    insert_feature(item, feature_col=feature_col)

    # Check that we did copy item (otherwise mongo adds an _id)
    assert "_id" not in item

    found = get_features(feature_col=feature_col, additional_filter=item)
    assert len(found) == 1

    delete_features(feature_col=feature_col, location_id=item['location_id'])

    found = get_features(feature_col=feature_col, additional_filter=item)
    assert len(found) == 0

    insert_features([item, item], feature_col=feature_col)
    found = get_features(feature_col=feature_col, additional_filter=item)
    assert len(found) == 2

    delete_features(feature_col=feature_col, location_id=item["location_id"])
    insert_features(features=features, feature_col=feature_col)
    found = get_features(feature_col=feature_col,
                        date_from=dt.date.today())
    assert len(list(found)) == 2

    found = get_features(feature_col=feature_col,
                        date_to=dt.date.today() - dt.timedelta(days=1))
    assert len(list(found)) == 2

    found = get_features(feature_col=feature_col,
                        date_from=dt.date.today(),
                        date_to=dt.date.today() - dt.timedelta(days=1))
    assert len(list(found)) == 0


    insert_features([item], feature_col=feature_col)

    # Try creating index
    # It should fail since there are two similar items
    with pytest.raises(DuplicateKeyError):
        db.create_feature_index(feature_col=feature_col)

    delete_features(feature_col=feature_col, location_id=item['location_id'])

    # It should now succed
    db.create_feature_index(feature_col=feature_col)

    # It should failed
    insert_feature(item, feature_col=feature_col)
    with pytest.raises(DuplicateKeyError):
        insert_feature(item, feature_col=feature_col, drop_if_exists=False)

    # Unless we ask to drop first
    insert_feature(item, feature_col=feature_col, drop_if_exists=True)


def test_delete_existing(feature_col, features):

    insert_features(features, feature_col=feature_col)
    db.create_feature_index(feature_col=feature_col)

    with pytest.raises(BulkWriteError):
        insert_features(features, feature_col=feature_col, drop_if_exists=False)

    insert_features(features, feature_col=feature_col, drop_if_exists=True)


def test_enforce_schema(db_test, feature_col):

    # First insert a valid item
    with open('satemdata/feature/tests/data/feature_valid.json', 'r') as j:
        feature_valid = json.loads(j.read())

    insert_feature(feature_valid, feature_col=feature_col)

    db.enforce_schema(db=db_test)

    insert_feature(feature_valid, feature_col=feature_col)

    with open('satemdata/feature/tests/data/feature_invalid1.json', 'r') as j:
        feature_invalid1 = json.loads(j.read())

    with pytest.raises(WriteError):
        insert_feature(feature_invalid1, feature_col=feature_col)


def test_create_index(feature_col):
    def get_matching_indexes():
        indexes = feature_col.index_information()
        return [x for x in indexes.values() if x.get('unique') is True
                and set([y[0] for y in x['key']]) == set(FEATURES_UNIQUE_COLS)]

    assert len(get_matching_indexes()) == 0

    db.create_feature_index(feature_col=feature_col)
    assert len(get_matching_indexes()) == 1

    db.create_feature_index(feature_col=feature_col)
    assert len(get_matching_indexes()) == 1


def test_get_features(feature_col, features):

    feature_col.delete_many({})
    found = get_features(location_id=features[0]['location_id'], feature_col=feature_col)
    assert len(found) == 0

    insert_features(features, feature_col=feature_col)

    found = get_features(location_id=features[0]['location_id'], feature_col=feature_col)
    assert len(list(found)) == 2

    found = get_features(date=features[0]['date'], feature_col=feature_col)
    assert len(list(found)) == 2

    found = get_features(location_id=features[0]['location_id'],
                        date=features[0]['date'],
                        feature_col=feature_col)
    assert len(list(found)) == 1