from datetime import timedelta
from Athena.signals.signal_bar import GeneralBar

__author__ = 'zed'


def ma_test_case_sub():

    my_signal = GeneralBar('au1606', timedelta(minutes=5))
    my_signal.start()

if __name__ == '__main__':
        ma_test_case_sub()