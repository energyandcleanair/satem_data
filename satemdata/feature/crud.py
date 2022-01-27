import datetime as dt
from . import db
from .utils import clean_feature
from .utils import clean_date


def insert_feature(feature, feature_col=db.get_feature_col()):
    feature = feature.copy() #MongoDB will add _id to it
    feature = clean_feature(feature)
    return feature_col.insert_one(feature)


def insert_features(features, feature_col=db.get_feature_col()):
    features = [clean_feature(x.copy()) for x in features]
    return feature_col.insert_many(features)


def get_features(location_id=None, date=None, additional_filter={}, feature_col=db.get_feature_col()):
    filter = additional_filter.copy()
    if "date" in filter:
        filter['date'] = clean_date(filter['date'])

    if location_id:
        filter['location_id'] = location_id

    if date:
        filter['date'] = clean_date(date)

    return list(feature_col.find(filter))


def delete_features(location_id, date=None, feature_col=db.get_feature_col()):
    filter = {'location_id': location_id}

    if date:
        filter['date'] = clean_date(date)

    return feature_col.delete_many(filter=filter)