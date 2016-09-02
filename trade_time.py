from datetime import datetime, timedelta

from Athena.settings import AthenaConfig
from Athena.utils import filetime_to_dt


def is_in_trade_time(dt, instrument):
    """

    :param dt:
    :param instrument:
    :return:
    """
    exchange = AthenaConfig.hermes_exchange_mapping[instrument]
    day_start = datetime(dt.year, dt.month, dt.day)

    if exchange == 'SHFE':
        # uft
        if day_start <= dt \
                <= day_start + timedelta(hours=2, minutes=30):
            # 00:00 - 02:30
            return True
        elif day_start + timedelta(hours=9) <= dt \
                <= day_start + timedelta(hours=10, minutes=15):
            # 09:00 - 10:15
            return True
        elif day_start + timedelta(hours=10, minutes=30) <= dt \
                <= day_start + timedelta(hours=11, minutes=30):
            # 10:30 - 11:30
            return True
        elif day_start + timedelta(hours=13, minutes=29) <= dt \
                <= day_start + timedelta(hours=15):
            # 13:30 - 15:00
            return True
        elif day_start + timedelta(hours=21) <= dt \
                <= day_start + timedelta(hours=24):
            # 21:00 - 24:00
            return True
        else:
            return False

    elif exchange == 'SGE':
        # ksd
        if day_start <= dt \
                <= day_start + timedelta(hours=2, minutes=30):
            # 00:00 - 02:30
            return True
        elif day_start + timedelta(hours=9) <= dt \
                <= day_start + timedelta(hours=11, minutes=30):
            # 09:00 - 11:30
            return True
        elif day_start + timedelta(hours=13, minutes=29) <= dt \
                <= day_start + timedelta(hours=15, minutes=30):
            # 13:30 - 15:30
            return True
        elif day_start + timedelta(hours=20) <= dt \
                <= day_start + timedelta(hours=24):
            # 20:00 - 24:00
            return True
        else:
            return False

    elif exchange == 'CME':
        if day_start + timedelta(hours=5) < dt \
                < day_start + timedelta(hours=5, minutes=30):
            return False
        else: return True
    else:
        raise ValueError

if __name__ == '__main__':
    d = filetime_to_dt(131145893910000000)
    print(d)
    print(is_in_trade_time(d, 'Au(T+D)'))