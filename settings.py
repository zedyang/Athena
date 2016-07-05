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
    # the host name
    ATHENA_SQL_HOST_NAME = '127.0.0.1'
    # the port
    ATHENA_SQL_PORT = 1433
    # login name for SQL server authentication
    ATHENA_SQL_LOGIN_NAME = 'intern_01'
    # password for SQL server authentication
    ATHENA_SQL_LOGIN_PWD = '123456'
    # name of the database that saves historical market data.
    ATHENA_SQL_DB_NAME = 'db_1'

    # The following section is to configure Redis database connection.
    # ---------------------------------------------------------------------
    ATHENA_REDIS_HOST_NAME = '127.0.0.1'   # the host name
    ATHENA_REDIS_PORT = 6379               # the port

    # The following section lists SQL Server table names.
    # ---------------------------------------------------------------------
    ATHENA_SQL_TABLE_NAME_TICK = 'tick_history'
    ATHENA_SQL_TABLE_NAME_BAR_1M = 'one_minute_history'
    # TODO: finish this for all tables (Zed, 2016-07-02)

    # The following section is to let Athena know data formatting/structure
    # in SQL Server
    # ---------------------------------------------------------------------
    # the headers (column names) of tick tables in SQL server.
    ATHENA_SQL_TABLE_HEADERS_TICK = (
        'SECURITY', 'DATE', 'TIME', 'LOCAL_TIME', 'MD_TYPE',
        'MD_SUBTYPE', 'PRICE', 'VOLUME', 'VALUE')

    # the headers (column names) of bar tables in SQL server
    ATHENA_SQL_TABLE_HEADERS_BAR = (
        'SECURITY', 'DATE', 'TIME', 'LOCAL_TIME',
        'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE',
        'VOLUME', 'VALUE'
    )

    # instrument column name
    ATHENA_SQL_TABLE_FIELD_INSTRUMENT = ATHENA_SQL_TABLE_HEADERS_TICK[0]
    # timestamp column name
    ATHENA_SQL_TABLE_FIELD_DATETIME = ATHENA_SQL_TABLE_HEADERS_TICK[3]
    # timestamp column format
    ATHENA_SQL_DT_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
    # bid ask column name
    ATHENA_SQL_TABLE_FIELD_SUBTYPE = ATHENA_SQL_TABLE_HEADERS_TICK[5]
    ATHENA_SQL_TABLE_FIELD_PRICE = ATHENA_SQL_TABLE_HEADERS_TICK[6]
    ATHENA_SQL_TABLE_FIELD_VOLUME = ATHENA_SQL_TABLE_HEADERS_TICK[7]

    # The following section lists all available instrument names in SQL server.
    # ---------------------------------------------------------------------
    ATHENA_SQL_INSTRUMENTS = (
        'Au(T+D)',
        'mAu(T+D)'
        'Au(T+N1)',
        'Au(T+N2)',
        'au1512',
        'au1606',
        'Au9995',
        'Au9999',
        'AUAM5',
        'AUAZ5',
        'AUAM5 Comdty',
        'EURUSD BGNL Cumcy',
        'JPYUSD BGNL Cumcy',
        'XAUUSD BGNL Cumcy',
        'IFB1',
        'GC1 Comdty',
        'SHGFAUTD Comdty',
        'SHGFAUTD Index'
    )

    # The following section specifies which cache field (database index)
    # in Redis is to save * type of data.
    # ---------------------------------------------------------------------
    ATHENA_REDIS_MD_DB_INDEX = 0       # market data: db0
    ATHENA_REDIS_SIGNAL_DB_INDEX = 3   # signals: db3

    # The following section set the name of directories in Redis
    # ---------------------------------------------------------------------
    ATHENA_REDIS_MD_DIR = 'md_backtest'

    # The following specified maximum counts of historical records
    # stored in redis
    # ---------------------------------------------------------------------
    ATHENA_REDIS_MD_MAX_RECORDS = 1e9       # maximum # of records
    ATHENA_REDIS_MD_MAX_DIGITS = 9          # # of dights
    ATHENA_REDIS_MD_END_FLAG = 'md_end'     # the flag marks end of md records

    @staticmethod
    def redis_historical_md_dir(instrument_list, begin, end):
        """
        Name of directory in Redis to store backtest md.
        :param instrument_list: list of str, names of instruments
        :param begin: datetime.datetime object
        :param end: datetime.datetime object
        :return:
        """
        begin_str = begin.strftime(AthenaConfig.ATHENA_SQL_DT_FORMAT)
        end_str = end.strftime(AthenaConfig.ATHENA_SQL_DT_FORMAT)
        return AthenaConfig.ATHENA_REDIS_MD_DIR + '_({})_{}_{}'.format(
            ','.join(instrument_list), begin_str, end_str)

    @staticmethod
    def show():
        print('----------------------------------------------')
        print('|            Athena Configurations           |')
        print('----------------------------------------------')
    # TODO: finish this prompt when all config items are specified.

if __name__ == '__main__':
    print(vars(AthenaConfig()))
    AthenaConfig.show()