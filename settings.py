from Athena.utils import map_to_kl_dir, map_to_md_dir, create_equiv_classes

__author__ = 'zed'


class AthenaConfig(object):
    """
    This is the configuration object for Athena Python-Analytic module.

    All the configurations are stored as a class attributes.
    Class __str__ and __repr__ methods are redefined to
    print out configurations.

    We simply do not need instances of AthenaConfig,
    hence __init__ method is missing.
    """

    # The following section is to configure MS SQL server connection
    # ---------------------------------------------------------------------
    sql_host = '127.0.0.1'
    sql_port = 1433
    sql_usr = 'intern_01'
    sql_pwd = '123456'

    sql_historical_db = 'Athena_test_db'
    sql_test_table = 'md160713'

    class TickFields(object):
        # proper headers of tick data used in Athena (Python PEP-8)
        tick_headers = (
            'trade_day', 'ex_update_time', 'local_update_time', 'category',
            'contract', 'last_price', 'bid', 'bid_vol', 'ask', 'ask_vol',
            'average_price', 'highest_price', 'lowest_price', 'pre_close',
            'open_interest', 'volume'
        )
        (day, ex_time, local_time, category, contract, last_price,
         bid, bid_vol, ask, ask_vol, avg_price, high_price, low_price,
         pre_close, open_int, volume) = tick_headers

    class KLineFields(object):
        # proper headers of kline data used in Athena (Python PEP-8)
        kline_headers = (
            'ex_open_time', 'open_time', 'end_time',
            'contract', 'open', 'high', 'low', 'close', 'volume',
            'open_interest', 'duration', 'duration_specifier', 'count'
        )
        (ex_open_time, open_time, end_time, contract,
         open_price, high_price, low_price, close_price, volume,
         open_int, duration, duration_specifier, count) = kline_headers

    dt_format = '%Y-%m-%d %H:%M:%S'
    sql_storage_dt_format = '%Y-%m-%d %H:%M:%S.%f'

    sql_instruments_list = [
        'ag1607',
        'ag1608',
        'ag1609',
        'ag1610',
        'ag1611',
        'ag1612',
        'ag1701',
        'ag1702',
        'ag1703',
        'ag1704',
        'ag1705',
        'ag1706'
        'Au(T+D)',
        'mAu(T+D)'
        'Au(T+N1)',
        'Au(T+N2)',
        'au1612',
        'au1706',
        'Au99.95',
        'Au99.99'
    ]

    # The following section is to configure Redis database connection.
    # ---------------------------------------------------------------------
    redis_host = '127.0.0.1'
    redis_port = 6379

    hist_stream_db_index = 3
    athena_db_index = 1
    daily_migration_cache_db_index = 10

    redis_md_dir = 'md:md_backtest'
    redis_temp_hist_stream_dir = 'temp_stream:hermes'

    redis_md_max_records = 1e9
    redis_key_max_digits = 9
    redis_md_end_flag = 'md_end'

    # The following section is to configure Hermes raw data stream
    # ---------------------------------------------------------------------
    class HermesTickFields(object):
        # headers of hermes raw tick data.
        hermes_tick_headers = (
            'day', 'systime', 'subtime', 'lastprice',
            'volume', 'openinterest', 'bid1', 'bidvol1', 'ask1', 'askvol1'
        )
        (day, ex_time, local_time, last_price,
         volume, open_int, bid, bid_vol, ask, ask_vol) = hermes_tick_headers

    class HermesKLineFields(object):
        # headers of hermes raw k-line data
        hermes_kline_headers = (
            'openwndtime', 'opensystime', 'dur', 'open', 'close',
            'high', 'low', 'volume', 'openinterest'
        )
        (open_time, ex_open_time, duration, open_price, close_price,
         high_price, low_price, volume, open_int) = hermes_kline_headers

    # duration specifiers
    hermes_kl_dur = ['clock', '1m', '3m', '5m', '10m', '15m']
    hermes_kl_dur_to_seconds = {
        'clock': 3,
        '1m': 60,
        '3m': 180,
        '5m': 300,
        '10m': 600,
        '15m': 900
    }

    # the inverse mapping
    hermes_kl_seconds_to_dur = dict()
    for k,v in hermes_kl_dur_to_seconds.items():
        hermes_kl_seconds_to_dur[v] = k

    class HermesMdDirectory(object):
        # Data directory prefix of different hermes md APIs.
        ksd_md = 'md.ksdreal.'
        ksd_kl = 'kl.ksdreal.'
        nanhua_md = 'md.nanhua.'
        nanhua_kl = 'kl.nanhua.'
        ctp_md = 'md.ctpnow.'
        ctp_kl = 'kl.ctpnow.'

    class HermesInstrumentsList(object):
        # Instrument list that is included in different APIs
        ksd = [
            'Ag(T+D)',
            'Ag99.9',
            'Ag99.99',
            'Au(T+D)',
            'Au(T+N1)',
            'Au(T+N2)',
            'mAu(T+D)',
            'Au100g',
            'Au50g',
            'Au99.95',
            'Au99.99',
            'Au99.5',
            'iAu100g',
            'iAu99.5',
            'iAu99.99'
        ]

        ctp = [
            'au1608',
            'au1609',
            'au1610',
            'au1611',
            'au1612',
            'au1701',
            'au1702',
            'au1703',
            'au1704',
            'au1705',
            'au1706',
            'ag1608',
            'ag1609',
            'ag1610',
            'ag1611',
            'ag1612',
            'ag1701',
            'ag1702',
            'ag1703',
            'ag1704',
            'ag1705',
            'ag1706'
        ]

        uft = ctp

        nanhua = [
            'GC1608',
            'GC1609',
            'GC1610',
            'GC1611',
            'GC1612',
            'GC1701',
            'GC1702',
            'GC1703',
            'GC1704',
            'GC1705',
            'GC1706'
        ]

        ag = [
            'Ag(T+D)',
            'Ag99.9',
            'Ag99.99',
            'ag1608',
            'ag1609',
            'ag1610',
            'ag1611',
            'ag1612',
            'ag1701',
            'ag1702',
            'ag1703',
            'ag1704',
            'ag1705',
            'ag1706'
        ]

        all = ctp + ksd + nanhua

    # create instrument symbol -> md and kline data directory mapping.
    associative_list_md = (
        (HermesInstrumentsList.ctp, HermesMdDirectory.ctp_md),
        (HermesInstrumentsList.ksd, HermesMdDirectory.ksd_md),
        (HermesInstrumentsList.nanhua, HermesMdDirectory.nanhua_md)
    )

    # md directory mapping
    hermes_md_mapping = map_to_md_dir(associative_list_md)

    associative_list_kl = (
        (HermesInstrumentsList.ctp, HermesMdDirectory.ctp_kl),
        (HermesInstrumentsList.ksd, HermesMdDirectory.ksd_kl),
        (HermesInstrumentsList.nanhua, HermesMdDirectory.nanhua_kl)
    )

    # kline directory mapping.
    hermes_kl_mapping = dict()
    for dur in hermes_kl_dur:
        hermes_kl_mapping[dur] = map_to_kl_dir(associative_list_kl, dur)

    # create instrument symbol -> exchange mapping.
    associative_list_exchange = (
        (HermesInstrumentsList.ctp, 'SHFE'),
        (HermesInstrumentsList.ksd, 'SGE'),
        (HermesInstrumentsList.nanhua, 'CME')
    )
    hermes_exchange_mapping = create_equiv_classes(associative_list_exchange)

    # create instrument symbol -> category mapping
    hermes_category_mapping = dict()
    for symbol in HermesInstrumentsList.all:
        if symbol in HermesInstrumentsList.ag:
            hermes_category_mapping[symbol] = 'ag'
        else:
            hermes_category_mapping[symbol] = 'au'

    hermes_db_index = 0
    hermes_md_sep_char = '|'

    # The following section is to configure Athena dictionary data structure.
    # ---------------------------------------------------------------------
    class AthenaMessageTypes(object):
        # value of tag field in athena db's dict data
        md = 'md'
        kl = 'kl'

    @staticmethod
    def redis_historical_md_dir(instrument_list, begin, end):
        """
        Name of directory in Redis to store backtest md.
        :param instrument_list: list of str, names of instruments
        :param begin: datetime.datetime object
        :param end: datetime.datetime object
        :return:
        """
        begin_str = begin.strftime(AthenaConfig.dt_format)
        end_str = end.strftime(AthenaConfig.dt_format)
        return AthenaConfig.redis_md_dir + '_({})_{}_{}'.format(
            ','.join(instrument_list), begin_str, end_str)


class AthenaProperNames(object):
    """
    A list of proper names that are frequently used in this module.
    """
    long = 'long'
    short = 'short'

    bid = 'bid'
    ask = 'ask'

    md_message_type = 'md'
    order_message_type = 'order'
    signal_message_type = 'signal'
    portfolio_message_type = 'portfolio'

