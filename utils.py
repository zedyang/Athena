from datetime import datetime, timedelta, tzinfo
from calendar import timegm

__author__ = 'zed'

MAX_DIGITS = 9


def append_digits_suffix_for_redis_key(prefix, counter):
    """
    return a structured string as a key in redis db.
    prefix is the name of folder, # of digits is in
    AthenaConfig.redis_key_max_digits
    :param prefix: string
    :param counter: string
    :return: string like dir:00000###,
    """
    return (
        prefix + ':' +
        (MAX_DIGITS - len(str(counter)))*'0' +
        str(counter)
    )


def create_equiv_classes(associative_list):
    """
    create equivalent class from associative list.
    :param associative_list:
    :return:
    """
    equiv_dict = dict()
    for keys, val in associative_list:
        for k in keys:
            equiv_dict[k] = val
    return equiv_dict


def map_to_md_dir(associative_list):
    """
    map from (instrument, API) associative list
    to {instrument -> md_directory} mapping.
    :param associative_list:
    :return:
    """
    equiv_dict = create_equiv_classes(associative_list)
    for k,v in equiv_dict.items():
        equiv_dict[k] = v + k
    return equiv_dict


def map_to_kl_dir(associative_list, dur_specifier):
    """

    :param associative_list:
    :param dur_specifier: string, duration specifier of the kline,
        - 'clock': 3 seconds
        - '1m'
        - '3m' ...
    :return:
    """
    equiv_dict = create_equiv_classes(associative_list)
    for k,v in equiv_dict.items():
        equiv_dict[k] = v + k + '.' + dur_specifier
    return equiv_dict


# -------------------------------------------------------------------------
EPOCH_AS_FILETIME = 116444736000000000  # January 1, 1970 as MS file time
HUNDREDS_OF_NANOSECONDS = 10000000

ZERO = timedelta(0)
HOUR = timedelta(hours=1)


class UTC(tzinfo):
    """UTC"""
    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO


utc = UTC()


def dt_to_filetime(dt, tz_adjustment=8):
    """
    Converts a datetime to Microsoft filetime format. If the object is
    time zone-naive, it is forced to UTC before conversion.
    """
    dt = dt - timedelta(hours=tz_adjustment)
    if (dt.tzinfo is None) or (dt.tzinfo.utcoffset(dt) is None):
        dt = dt.replace(tzinfo=utc)
    ft = EPOCH_AS_FILETIME + (timegm(dt.timetuple()) * HUNDREDS_OF_NANOSECONDS)
    return ft + (dt.microsecond * 10)


def filetime_to_dt(ft, tz_adjustment=8):
    """
    Converts a Microsoft filetime number to a Python datetime. The new
    datetime object is time zone-naive but is equivalent to tzinfo=utc.
    :param ft: int, filetime representation
    :param tz_adjustment: int, timezone adjustment.
    :return:
    """
    # Get seconds and remainder in terms of Unix epoch
    (s, ns100) = divmod(ft - EPOCH_AS_FILETIME, HUNDREDS_OF_NANOSECONDS)
    # Convert to datetime object
    dt = datetime.utcfromtimestamp(s)

    # Add remainder in as microseconds. Python 3.2 requires an integer
    dt = dt.replace(microsecond=(ns100 // 10))
    return dt + timedelta(hours=tz_adjustment)

