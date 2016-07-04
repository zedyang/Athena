from Athena.signals.signal import NaiveSingleInstrumentSignal
from Athena.apis.database_api import RedisAPI

__author__ = 'zed'


def naive_test_case_sub():
    redis_api = RedisAPI(db=0)
    redis_api.set_dict('au1606:800', {'begin': 'begin'})

    my_signal = NaiveSingleInstrumentSignal('au1606')
    my_signal.start()

if __name__ == '__main__':
    naive_test_case_sub()
