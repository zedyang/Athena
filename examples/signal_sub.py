import multiprocessing
import time
from datetime import timedelta

from Athena.signals.signal_bar_bid_ask import GeneralBar
from Athena.examples.backtest_pub import *

__author__ = 'zed'


def naive_signal_sub():
    my_signal = GeneralBar(instrument='GC1612',
                           period=timedelta(minutes=1),
                           tag='bar')
    my_signal.start()

if __name__ == '__main__':
    naive_signal_sub()


