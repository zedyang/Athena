import time

from Athena.market_data_handler.market_data_handler import TickDataHandler
from Athena.market_data_handler.market_data_handler_test import test_data, \
    test_keys, test_data_longer_single_instrument, test_keys_longer
from Athena.apis.database_api import RedisAPI

__author__ = 'zed'

test_case = 'ma'


def naive_test_case_pub():
    """
    Test the naive signal.
    :return:
    """
    redis_api = RedisAPI(db=0)
    redis_api.flush_db()

    for d in test_data:
        redis_api.set_dict(list(d.keys())[0], list(d.values())[0])

    instruments_list = ['Au(T+D)', 'au1606']
    data_handler = TickDataHandler(instruments_list)

    for k in test_keys:
        flag = data_handler.stream_bar_by_key(k)
        print('[pub]: !')
        time.sleep(1)


def ma_test_case_pub():
    """

    :return:
    """
    redis_api = RedisAPI(db=0)
    redis_api.flush_db()

    for d in test_data_longer_single_instrument:
        redis_api.set_dict(list(d.keys())[0], list(d.values())[0])

    instruments_list = ['au1606']
    data_handler = TickDataHandler(instruments_list)

    for k in test_keys_longer:
        flag = data_handler.stream_bar_by_key(k)
        print('[pub]: !')
        time.sleep(1)


if __name__ == '__main__':
    if test_case == 'naive':
        naive_test_case_pub()
    elif test_case == 'ma':
        ma_test_case_pub()


