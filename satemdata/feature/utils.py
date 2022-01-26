import datetime as dt

from . import DATE_FORMAT


def get_feature_date(feature):
    return dt.datetime.strptime(feature['date'], DATE_FORMAT)


def clean_date(date):
    if isinstance(date, dt.date) or isinstance(date, dt.datetime):
        date = date.strftime(DATE_FORMAT)
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
    required_fields = ["location_id", "date", "tropomi_no2"]
    missing = set(required_fields) - set(feature.keys())
    if missing:
        raise ValueError("Missing fields in feature: %s"% (missing,))

    return feature