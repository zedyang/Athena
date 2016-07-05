from Athena.signals.signal_donchian_channel import HighestHigh

__author__ = 'zed'


def ma_test_case_sub():

    my_signal = HighestHigh('bar_1m_au1606')
    my_signal.start()

if __name__ == '__main__':
        ma_test_case_sub()