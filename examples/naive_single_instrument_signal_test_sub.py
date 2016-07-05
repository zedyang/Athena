from Athena.signals.signal import NaiveSingleInstrumentSignal

__author__ = 'zed'


def naive_test_case_sub():

    my_signal = NaiveSingleInstrumentSignal('au1606')
    my_signal.start()

if __name__ == '__main__':
    naive_test_case_sub()
