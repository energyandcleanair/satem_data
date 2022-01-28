import datetime as dt
import pandas as pd

from . import db
from .utils import clean_feature
from .utils import clean_date


def insert_feature(feature, feature_col=None, drop_if_exists=False):
    if feature_col is None:
        feature_col = db.get_feature_col()

    feature = feature.copy() #MongoDB will add _id to it
    feature = clean_feature(feature)

    if drop_if_exists:
        delete_features(location_id=feature["location_id"],
                        date=feature["date"],
                        feature_col=feature_col)

    return feature_col.insert_one(feature)


def insert_features(features, feature_col=None, drop_if_exists=False):
    if feature_col is None:
        feature_col = db.get_feature_col()

    if drop_if_exists:
        location_dates = pd.DataFrame(features) \
            .groupby(['location_id']).date.unique() \
            .reset_index().to_dict(orient='records')
        for d in location_dates:
            delete_features(location_id=d["location_id"],
                            dates=d["date"],
                            feature_col=feature_col)

    features = [clean_feature(x.copy()) for x in features]
    return feature_col.insert_many(features)


def get_features(location_id=None, date=None, additional_filter={}, feature_col=None):
    if feature_col is None:
        feature_col = db.get_feature_col()

    filter = additional_filter.copy()
    if "date" in filter:
        filter['date'] = clean_date(filter['date'])

    if location_id:
        filter['location_id'] = location_id

    if date:
        filter['date'] = clean_date(date)

    return list(feature_col.find(filter))


def delete_features(location_id, date=None, dates=None, feature_col=None):
    if feature_col is None:
        feature_col = db.get_feature_col()

    filter = {'location_id': location_id}

    if date:
        filter['date'] = clean_date(date)

    if dates is not None:
        filter['date'] = {"$in": [clean_date(date) for date in dates]}

    return feature_col.delete_many(filter=filter)