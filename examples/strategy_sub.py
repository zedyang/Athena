from Athena.strategies.cta_strategy import CTAStrategy1

__author__ = 'zed'


def naive_test_case_sub():

    my_strategy = CTAStrategy1(
        [
            'md:GC1608',
            'kl:GC1608.1m',
            'signal:donchian.20.kl.GC1608.1m',
            'signal:ma.kl.GC1608.1m',
            'signal:trailing.stop'
        ]
    )
    my_strategy.start()

if __name__ == '__main__':
    naive_test_case_sub()