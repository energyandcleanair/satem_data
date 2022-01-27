import datetime as dt
from . import db
from .utils import clean_result
from .utils import clean_date


def insert_result(result, result_col=None):
    if result_col is None:
        result_col = db.get_result_col()

    result = result.copy() #MongoDB will add id to it
    result = clean_result(result)
    return result_col.insert_one(result)


def insert_results(results, result_col=None):
    if result_col is None:
        result_col = db.get_result_col()

    results = [clean_result(x.copy()) for x in results]
    return result_col.insert_many(results)


def get_results(location_id=None, date=None, additional_filter={}, result_col=None):
    if result_col is None:
        result_col = db.get_result_col()

    filter = additional_filter.copy()
    if "date" in filter:
        filter['date'] = clean_date(filter['date'])

    if location_id:
        filter['location_id'] = location_id

    if date:
        filter['date'] = clean_date(date)

    return list(result_col.find(filter))


def delete_results(location_id, date=None, result_col=None):
    if result_col is None:
        result_col = db.get_result_col()

    filter = {'location_id': location_id}

    if date:
        filter['date'] = clean_date(date)

    return result_col.delete_many(filter=filter)