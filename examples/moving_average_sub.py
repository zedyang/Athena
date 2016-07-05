from Athena.signals.signal_ma import MovingAverage

__author__ = 'zed'


def ma_test_case_sub():

    my_signal = MovingAverage('bar_1m_au1606', 10)
    my_signal.start()

if __name__ == '__main__':
        ma_test_case_sub()