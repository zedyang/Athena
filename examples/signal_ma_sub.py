from Athena.signals.moving_average import MovingAverageBatch

__author__ = 'zed'


def ma_test_case_sub():
    my_signal = MovingAverageBatch(['kl:GC1608.1m'],
                                   window_widths=(36, 48))
    my_signal.start()

if __name__ == '__main__':
    ma_test_case_sub()
