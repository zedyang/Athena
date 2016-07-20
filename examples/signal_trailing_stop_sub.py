from Athena.signals.trailing_stop import TrailingStop

__author__ = 'zed'


def stop_test_case_sub():
    my_signal = TrailingStop(
        sub_channels=[
            'kl:GC1608.1m',
            'strategy:cta_1'
        ],
        target_strategy='strategy:cta_1',
    )
    my_signal.start()

if __name__ == '__main__':
    stop_test_case_sub()