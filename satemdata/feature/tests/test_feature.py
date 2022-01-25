import datetime as dt
import pytest
from pymongo.errors import DuplicateKeyError


import feature
from feature import db
from feature import FEATURES_UNIQUE_COLS
from feature import FEATURES_COLLECTION
from feature import DB_SATEM_TEST
from feature import DATE_FORMAT


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
            'facility_id': 'test001',
            'date': dt.date.today().strftime(DATE_FORMAT)
        },{
            'facility_id': 'test001',
            'date': (dt.date.today()-dt.timedelta(days=1)).strftime(DATE_FORMAT)
        },{
            'facility_id': 'test002',
            'date': dt.date.today().strftime(DATE_FORMAT)
        },{
            'facility_id': 'test002',
            'date': (dt.date.today()-dt.timedelta(days=1)).strftime(DATE_FORMAT)
        }]


def test_crud_feature(feature_col, features):

    item = features[0]

    feature_col.insert_one(item.copy())
    found = feature_col.find({}, item)
    assert len(list(found))==1

    feature_col.delete_many(filter={'facility_id': item['facility_id']})

    found = feature_col.find({}, item)
    assert len(list(found)) == 0

    feature_col.insert_many([item.copy(), item.copy()])
    found = feature_col.find({}, item)
    assert len(list(found)) == 2

    # Try creating index
    # It should fail since there are two similar items
    with pytest.raises(DuplicateKeyError):
        db.create_feature_index(feature_col=feature_col)

    feature_col.delete_one(filter={'facility_id': item['facility_id']})

    # It should now succed
    db.create_feature_index(feature_col=feature_col)

    # It should failed
    with pytest.raises(DuplicateKeyError):
        feature_col.insert_one(item.copy())


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
    found = feature.get_features(facility_id=features[0]['facility_id'], feature_col=feature_col)
    assert len(found) == 0

    feature.insert_features(features, feature_col=feature_col)

    found = feature.get_features(facility_id=features[0]['facility_id'], feature_col=feature_col)
    assert len(list(found)) == 2

    found = feature.get_features(date=features[0]['date'], feature_col=feature_col)
    assert len(list(found)) == 2

    found = feature.get_features(facility_id=features[0]['facility_id'],
                                 date=features[0]['date'],
                                 feature_col=feature_col)
    assert len(list(found)) == 1