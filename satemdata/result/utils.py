import datetime as dt

from . import DATE_FORMAT


def get_result_date(result):
    return dt.datetime.strptime(result['date'], DATE_FORMAT)


def clean_date(date):
    if isinstance(date, dt.date) or isinstance(date, dt.datetime):
        return date
    else:
        try:
            date = dt.datetime.strptime(date, DATE_FORMAT)
        except ValueError:
            raise ValueError("date should be with format %s (actual value: %s)" % (DATE_FORMAT, date))
    return date


def clean_result(result):
    """
    Clean (and check) a result
    :param result:
    :return: cleaned result
    """

    if not isinstance(result, dict):
        raise ValueError("result should be a dictionary")

    # Ensure date is in the right format
    result['date'] = clean_date(result['date'])

    # Check required fields are here
    required_fields = ["location_id", "date", "window", "method"]
    missing = set(required_fields) - set(result.keys())
    if missing:
        raise ValueError("Missing fields in result: %s"% (missing,))

    return result