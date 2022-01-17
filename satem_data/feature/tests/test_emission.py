from feature import db
import datetime as dt
from feature import FEATURES_UNIQUE_COLS


def test_get_collection():
    feature_col = db.get_feature_collection()

    item = {
        'facility_id': 'test001',
        'date': dt.date.today().strftime("%Y%m%d")
    }
    feature_col.insert_one(item)
    found = feature_col.find({}, item)
    assert len(list(found))==1

    feature_col.delete_one(filter={'facility_id': 'test001'})

    found = feature_col.find({}, item)
    assert len(list(found)) == 0


def test_create_index():
    feature_db = db.get_database()
    TEST_COLLECTION = "feature_test"
    feature_db.drop_collection(TEST_COLLECTION)
    feature_col_test = feature_db[TEST_COLLECTION]

    def get_matching_indexes():
        indexes = feature_col_test.index_information()
        return [x for x in indexes.values() if x.get('unique') is True
                and set([y[0] for y in x['key']]) == set(FEATURES_UNIQUE_COLS)]

    assert len(get_matching_indexes()) == 0

    db.create_feature_index(feature_col=feature_col_test)
    assert len(get_matching_indexes()) == 1

    db.create_feature_index(feature_col=feature_col_test)
    assert len(get_matching_indexes()) == 1

    feature_db.drop_collection(TEST_COLLECTION)

