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

    class SQLKlineFields(object):
        # table headers in SQL
        kline_headers = (
            'RowId',
            'TradingDay',
            'ExUpdateTime',
            'LocalUpdateTime',
            'ExchangeID',
            'Category',
            'Symbol',
            'TimeFrame',
            'Open',
            'High',
            'Low',
            'Close',
            'Volume',
            'Turnover',
            'OpenInterest',
            'Average',
            'TotalVolume',
            'TotalTurnover',
            'DayAveragePrice',
            'OpenTime',
            'HighTime',
            'LowTime',
            'CloseTime',
            'Rank',
            'UniqueIndex'
        )
        (id, day, ex_update_time, update_time, exchange,
         category, contract, duration,
         open_price, high_price, low_price, close_price,
         volume, turnover, open_interest, average_price,
         total_volume, total_turnover, daily_avg_price,
         open_time, high_time, low_time, close_time,
         rank, index) = kline_headers
        ohlc = (open_price, high_price, low_price, close_price)

    class SQLTickFields(object):
        # table headers in SQL
        tick_headers = (
            'RowId',
            'TradingDay',
            'ExUpdateTime',
            'LocalUpdateTime',
            'ExchangeID',
            'Category',
            'Symbol',
            'LastPrice',
            'BidPrice1',
            'BidPrice2',
            'BidPrice3',
            'BidPrice4',
            'BidPrice5',
            'BidPrice6',
            'BidPrice7',
            'BidPrice8',
            'BidPrice9',
            'BidPrice10',
            'BidVolume1',
            'BidVolume2',
            'BidVolume3',
            'BidVolume4',
            'BidVolume5',
            'BidVolume6',
            'BidVolume7',
            'BidVolume8',
            'BidVolume9',
            'BidVolume10',
            'AskPrice1',
            'AskPrice2',
            'AskPrice3',
            'AskPrice4',
            'AskPrice5',
            'AskPrice6',
            'AskPrice7',
            'AskPrice8',
            'AskPrice9',
            'AskPrice10',
            'AskVolume1',
            'AskVolume2',
            'AskVolume3',
            'AskVolume4',
            'AskVolume5',
            'AskVolume6',
            'AskVolume7',
            'AskVolume8',
            'AskVolume9',
            'AskVolume10',
            'AveragePrice',
            'HighestPrice',
            'LowestPrice',
            'PreClosePrice',
            'OpenInterest',
            'Volume',
            'Turnover',
            'Rank',
            'UniqueIndex'
        )
        (id, day, ex_update_time, local_update_time, exchange,
         category, contract, last_price,
         bid_1, bid_2, bid_3, bid_4, bid_5,
         bid_6, bid_7, bid_8, bid_9, bid_10,
         bid_vol_1, bid_vol_2, bid_vol_3, bid_vol_4, bid_vol_5,
         bid_vol_6, bid_vol_7, bid_vol_8, bid_vol_9, bid_vol_10,
         ask_1, ask_2, ask_3, ask_4, ask_5,
         ask_6, ask_7, ask_8, ask_9, ask_10,
         ask_vol_1, ask_vol_2, ask_vol_3, ask_vol_4, ask_vol_5,
         ask_vol_6, ask_vol_7, ask_vol_8, ask_vol_9, ask_vol_10,
         average_price, highest_price, lowest_price, preclose_price,
         open_int, volume, turnover, rank, index) = tick_headers

    class OrderFields(object):
        # proper headers of order data used in Athena (Python PEP-8)
        athena_order_headers = (
            'direction',
            'type',
            'subtype',
            'quantity',
            'price',
            'contract',
            'commission',
            'update_time',
            'bar_count'
        )
        (direction, type, subtype, quantity, price,
         contract, commission, update_time, bar_count) = athena_order_headers

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
    redis_host_local = '127.0.0.1'
    redis_host_remote_1 = '10.88.26.26'
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
            'day',
            'systime',
            'subtime',
            'updatems',
            'exchangeid',
            'contract',
            'lastprice',
            'bid1',
            'bid2',
            'bid3',
            'bid4',
            'bid5',
            'bid6',
            'bid7',
            'bid8',
            'bid9',
            'bid10',
            'bidvol1',
            'bidvol2',
            'bidvol3',
            'bidvol4',
            'bidvol5',
            'bidvol6',
            'bidvol7',
            'bidvol8',
            'bidvol9',
            'bidvol10',
            'ask1',
            'ask2',
            'ask3',
            'ask4',
            'ask5',
            'ask6',
            'ask7',
            'ask8',
            'ask9',
            'ask10',
            'askvol1',
            'askvol2',
            'askvol3',
            'askvol4',
            'askvol5',
            'askvol6',
            'askvol7',
            'askvol8',
            'askvol9',
            'askvol10',
            'avgprx',
            'openprx',
            'highprx',
            'lowprx',
            'closeprx',
            'clearprx',
            'preclearprx',
            'precloseprx',
            'openinterest',
            'preopeninterest',
            'volume',
            'turnover',
            'key'
        )
        (
            day,
            ex_time,
            local_time,
            update_ms,
            exchange,
            contract,
            last_price,
            bid_1,
            bid_2,
            bid_3,
            bid_4,
            bid_5,
            bid_6,
            bid_7,
            bid_8,
            bid_9,
            bid_10,
            bid_vol_1,
            bid_vol_2,
            bid_vol_3,
            bid_vol_4,
            bid_vol_5,
            bid_vol_6,
            bid_vol_7,
            bid_vol_8,
            bid_vol_9,
            bid_vol_10,
            ask_1,
            ask_2,
            ask_3,
            ask_4,
            ask_5,
            ask_6,
            ask_7,
            ask_8,
            ask_9,
            ask_10,
            ask_vol_1,
            ask_vol_2,
            ask_vol_3,
            ask_vol_4,
            ask_vol_5,
            ask_vol_6,
            ask_vol_7,
            ask_vol_8,
            ask_vol_9,
            ask_vol_10,
            average_price,
            open_price,
            high_price,
            low_price,
            close_price,
            clear_price,
            pre_clear_price,
            pre_close_price,
            open_interest,
            pre_open_interest,
            volume,
            turnover,
            key
        ) = hermes_tick_headers

        asks = (
            ask_1,
            ask_2,
            ask_3,
            ask_4,
            ask_5,
            ask_6,
            ask_7,
            ask_8,
            ask_9,
            ask_10,
        )

        bids = (
            bid_1,
            bid_2,
            bid_3,
            bid_4,
            bid_5,
            bid_6,
            bid_7,
            bid_8,
            bid_9,
            bid_10,
        )

        ask_vols = (
            ask_vol_1,
            ask_vol_2,
            ask_vol_3,
            ask_vol_4,
            ask_vol_5,
            ask_vol_6,
            ask_vol_7,
            ask_vol_8,
            ask_vol_9,
            ask_vol_10,
        )

        bid_vols = (
            bid_vol_1,
            bid_vol_2,
            bid_vol_3,
            bid_vol_4,
            bid_vol_5,
            bid_vol_6,
            bid_vol_7,
            bid_vol_8,
            bid_vol_9,
            bid_vol_10,
        )

        floats = asks + bids + (last_price, average_price, open_price,
                                high_price, low_price, close_price,
                                clear_price, pre_clear_price, pre_close_price,)

        integers = ask_vols + bid_vols + (volume, update_ms, turnover,
                                          open_interest, pre_open_interest,)

    class HermesKLineFields(object):
        # headers of hermes raw k-line data

        hermes_kline_headers = (
            'day',
            'updatetime',
            'localtime',
            'exchangeid',
            'contract',
            'dur',
            'dur_specifier',
            'openprx',
            'highprx',
            'lowprx',
            'closeprx',
            'volume',
            'turnover',
            'openinterest',
            'avgprx',
            'precloseprx',
            'totalvolume',
            'totalturnover',
            'opentime',
            'hightime',
            'lowtime',
            'closetime',
            'key',
            'count'
        )
        (
            day,
            ex_time,
            local_time,
            exchange,
            contract,
            duration,
            duration_specifier,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            turnover,
            open_interest,
            average_price,
            pre_close_price,
            total_volume,
            total_turnover,
            open_time,
            high_time,
            low_time,
            close_time,
            key,
            count
        ) = hermes_kline_headers

        ohlc = (
            open_price,
            high_price,
            low_price,
            close_price
        )

        ohlc_time = (
            open_time,
            high_time,
            low_time,
            close_time
        )

        floats = ohlc + (average_price, pre_close_price,)
        integers = (volume, turnover, total_volume, total_turnover,
                    open_interest, duration)
        times = (ex_time, local_time, open_time,
                 high_time, low_time, close_time, 'timeframe')

    # duration specifiers
    hermes_kl_dur = [
        'clock',
        '1s', '3s', '5s', '10s', '15s', '30s',
        '1m', '3m', '5m', '10m', '15m', '30m',
        '1h', '3h'
    ]
    hermes_kl_dur_to_seconds = {
        '1s': 1,
        '3s': 3,
        '5s': 5,
        '10s': 10,
        '15s': 15,
        '30s': 30,
        '1m': 60,
        '3m': 180,
        '5m': 300,
        '10m': 600,
        '15m': 900,
        '30m': 1800,
        '1h': 3600,
        '3h': 10800
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
        uft_md = 'md.uftreal.'
        uft_kl = 'kl.uftreal.'

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
            'ag1706',
        ]

        uft = ctp

        nanhua = [
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
        (HermesInstrumentsList.uft, HermesMdDirectory.uft_md),
        (HermesInstrumentsList.ksd, HermesMdDirectory.ksd_md),
        (HermesInstrumentsList.nanhua, HermesMdDirectory.nanhua_md)
    )

    # md directory mapping
    hermes_md_mapping = map_to_md_dir(associative_list_md)

    associative_list_kl = (
        (HermesInstrumentsList.uft, HermesMdDirectory.uft_kl),
        (HermesInstrumentsList.ksd, HermesMdDirectory.ksd_kl),
        (HermesInstrumentsList.nanhua, HermesMdDirectory.nanhua_kl)
    )

    # kline directory mapping.
    hermes_kl_mapping = dict()
    for dur in hermes_kl_dur:
        hermes_kl_mapping[dur] = map_to_kl_dir(associative_list_kl, dur)

    # create instrument symbol -> exchange mapping.
    associative_list_exchange = (
        (HermesInstrumentsList.uft, 'SHFE'),
        (HermesInstrumentsList.ksd, 'SGE'),
        (HermesInstrumentsList.nanhua, 'CME')
    )
    hermes_exchange_mapping = create_equiv_classes(associative_list_exchange)

    # minimum price change
    hermes_tick_size_mapping = {
        'GC1608': 0.1,
        'GC1612': 0.1,
        'Au(T+D)': 0.01,
        'Ag(T+D)': 1,
        'au1612': 0.01
    }

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


