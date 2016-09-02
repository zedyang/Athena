from Athena.data_handler.data_pipe import HermesPipe

__author__ = 'zed'


if __name__ == '__main__':
    pipe = HermesPipe()
    pipe.add_instrument(
        instrument='Au(T+D)',
        kline_dur_specifiers=('1m', '3s')
    )
    pipe.add_instrument(
        instrument='au1612',
        kline_dur_specifiers=('1m', '3s')
    )
    pipe.add_instrument(
        instrument='GC1612',
        kline_dur_specifiers=('1m', '3s')
    )



    pipe.start_pipeline()