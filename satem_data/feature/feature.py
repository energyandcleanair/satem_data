import datetime as dt

from feature import DATE_FORMAT
from feature import db


def clean_date(date):
    if isinstance(date, dt.date) or isinstance(date, dt.datetime):
        date = date.strftime("%Y-%m-%d")
    else:
        try:
            date = dt.datetime.strptime(date, DATE_FORMAT).strftime(DATE_FORMAT)
        except ValueError:
            raise ValueError("date should be with format %s (actual value: %s)" % (DATE_FORMAT, date))
    return date


def clean_feature(feature):
    """
    Clean (and check) a feature
    :param feature:
    :return: cleaned feature
    """

    if not isinstance(feature, dict):
        raise ValueError("feature should be a dictionary")

    # Ensure date is in the right format
    feature['date'] = clean_date(feature['date'])

    # Check required fields are here
    required_fields = ["facility_id"]
    if set(required_fields) - set(feature.keys()):
        raise ValueError("Missing fields in feature")

    return feature


def insert_feature(feature):
    feature_col = db.get_feature_collection()
    feature = clean_feature(feature)
    feature_col.insert_one(feature)


def insert_features(features):
    feature_col = db.get_feature_collection()
    features = [clean_feature(x) for x in features]
    feature_col.insert_many(features)


def get_features(facility_id=None, date=None):
    feature_col = db.get_feature_collection()
    return []
