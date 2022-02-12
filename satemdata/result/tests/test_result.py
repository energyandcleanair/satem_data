import pytest
import json
from pymongo.errors import DuplicateKeyError, WriteError


import result
from result.crud import *
from result import RESULTS_UNIQUE_COLS
from result import RESULTS_COLLECTION
from result import DB_SATEM_TEST
from result import DATE_FORMAT


@pytest.fixture
def db_test():
    return db.get_result_db(DB_SATEM_TEST)


@pytest.fixture
def result_col():
    result_db = db.get_result_db(DB_SATEM_TEST)
    result_db.drop_collection(RESULTS_COLLECTION)
    result_col = db.get_result_col(db=result_db)
    return result_col


@pytest.fixture
def results():
    return [
        {
            'location_id': 'test001',
            'window': 'daily',
            'date': dt.date.today().strftime(DATE_FORMAT),
            'method': {'id': 'method_1'}
        },{
            'location_id': 'test001',
            'window': 'daily',
            'date': (dt.date.today()-dt.timedelta(days=1)).strftime(DATE_FORMAT),
            'method': {'id': 'method_1'}
        },{
            'location_id': 'test002',
            'window': 'daily',
            'date': dt.date.today().strftime(DATE_FORMAT),
            'method': {'id': 'method_1'}
        },{
            'location_id': 'test002',
            'window': 'daily',
            'date': (dt.date.today()-dt.timedelta(days=1)).strftime(DATE_FORMAT),
            'method': {'id': 'method_1'}
        }]


def test_crud_result(result_col, results):

    item = results[0]
    insert_result(item, result_col=result_col)

    # Check that we did copy item (otherwise mongo adds an _id)
    assert "_id" not in item

    found = get_results(result_col=result_col, additional_filter=item)
    assert len(list(found)) == 1

    delete_results(result_col=result_col, location_id=item['location_id'])

    found = get_results(result_col=result_col, additional_filter=item)
    assert len(list(found)) == 0

    insert_results([item, item], result_col=result_col)
    found = get_results(result_col=result_col, additional_filter=item)
    assert len(list(found)) == 2

    # Try creating index
    # It should fail since there are two similar items
    with pytest.raises(DuplicateKeyError):
        db.create_result_index(result_col=result_col)

    delete_results(result_col=result_col, location_id=item['location_id'])

    # It should now succed
    db.create_result_index(result_col=result_col)

    # It should failed
    insert_result(item, result_col=result_col)
    with pytest.raises(DuplicateKeyError):
        insert_result(item, result_col=result_col, drop_if_exists=False)

    # Unless we ask to drop it first
    insert_result(item, result_col=result_col, drop_if_exists=True)


def test_enforce_schema(db_test, result_col):

    # First insert a valid item
    with open('satemdata/result/tests/data/result_valid.json', 'r') as j:
        result_valid = json.loads(j.read())

    insert_result(result_valid, result_col=result_col)

    db.enforce_schema(db=db_test)

    insert_result(result_valid, result_col=result_col)

    with open('satemdata/result/tests/data/result_invalid1.json', 'r') as j:
        result_invalid1 = json.loads(j.read())

    with pytest.raises(WriteError):
        insert_result(result_invalid1, result_col=result_col)


def test_create_index(result_col):
    def get_matching_indexes():
        indexes = result_col.index_information()
        return [x for x in indexes.values() if x.get('unique') is True
                and set([y[0] for y in x['key']]) == set(RESULTS_UNIQUE_COLS)]

    assert len(get_matching_indexes()) == 0

    db.create_result_index(result_col=result_col)
    assert len(get_matching_indexes()) == 1

    db.create_result_index(result_col=result_col)
    assert len(get_matching_indexes()) == 1


def test_get_results(result_col, results):

    result_col.delete_many({})
    found = get_results(location_id=results[0]['location_id'], result_col=result_col)
    assert len(found) == 0

    insert_results(results, result_col=result_col)

    found = get_results(location_id=results[0]['location_id'], result_col=result_col)
    assert len(list(found)) == 2

    found = get_results(date=results[0]['date'], result_col=result_col)
    assert len(list(found)) == 2

    found = get_results(location_id=results[0]['location_id'],
                        date=results[0]['date'],
                        result_col=result_col)
    assert len(list(found)) == 1