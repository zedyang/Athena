from Athena.signals.donchian_channel import DonchianChannel

__author__ = 'zed'


def don_test_case_sub():
    my_signal = DonchianChannel(['kl:GC1608.1m'],
                                window=20)
    my_signal.start()

if __name__ == '__main__':
    don_test_case_sub()