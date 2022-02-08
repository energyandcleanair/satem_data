import datetime as dt
import pandas as pd


from . import db
from .utils import clean_result
from .utils import clean_date


def insert_result(result, result_col=None, drop_if_exists=False):
    if result_col is None:
        result_col = db.get_result_col()

    if drop_if_exists:
        # TODO make faster
        delete_results(location_id=result["location_id"],
                       date=result["date"],
                       additional_filter={"method.id": result["method"]["id"],
                                          "wind_m_s_threshold": result.get("wind_m_s_threshold"),
                                          "crosswind_km": result.get("crosswind_km"),
                                          "frequency": result.get("frequency")},
                       result_col=result_col)

    result = result.copy() #MongoDB will add id to it
    result = clean_result(result)
    return result_col.insert_one(result)


def insert_results(results, result_col=None, drop_if_exists=False):
    if result_col is None:
        result_col = db.get_result_col()

    if drop_if_exists:
        # TODO make faster
        for r in results:
            delete_results(location_id=r["location_id"],
                            date = r["date"],
                            additional_filter={"method.id": r["method"]["id"],
                                              "wind_m_s_threshold": r.get("wind_m_s_threshold"),
                                              "crosswind_km": r.get("crosswind_km"),
                                              "frequency": r.get("frequency")},
                            result_col = result_col)

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


def delete_results(location_id, date=None, additional_filter={}, result_col=None):
    if result_col is None:
        result_col = db.get_result_col()

    filter = additional_filter.copy()

    if location_id:
        filter['location_id'] = location_id

    if date:
        filter['date'] = clean_date(date)

    return result_col.delete_many(filter=filter)