import time

from Athena.settings import AthenaConfig
from Athena.apis.database_api import RedisAPI
from Athena.apis.mssql_api import SQLServerAPI

__author__ = 'zed'


class DataTransporter(object):
    """
    Data Transporter is a bridge between two databases
    (namely SQL server and Redis).

    Initially, all historical market data (all instruments and from the time
    being collected) is stored in SQL server, we now want to fetch a section
    from it, which is specified by a list of trading instruments and begin/
    ending time for the backtest.

    Data transporter's main functionality is to request data,
    taking instruments/backtest range as parameter; and then migrate this
    section into Redis cache.

    When backtest is finished, data transporter also helps to do the clean-up.
    """
    def __init__(self, mssql_api, redis_api):
        """
        Constructor.
        :param mssql_api: a reference to SQLServerAPI object.
        :param redis_api: a reference to RedisAPI object.
        """
        self.mssql_api = mssql_api
        self.redis_api = redis_api
        # make sure redis connection is at MD database.
        self.redis_api.assert_db_index(AthenaConfig.ATHENA_REDIS_MD_DB_INDEX)

    def transport_data(self, instruments_list, begin_time, end_time,
                       split_instruments=False,
                       table=AthenaConfig.ATHENA_SQL_TABLE_NAME_TICK,
                       message=True):
        """
        transport market data from SQL server with specified instruments list
        and time range to Redis Cache. Parameters are same as
        SQLServerAPI.select_market_data().

        :param instruments_list: list of strings,
            the instruments to be selected.
        :param begin_time: datetime.datetime object, beginning time.
        :param end_time: datetime.datetime object ending time.
        :param split_instruments: bool, whether to split the instruments.
            * Default = False
            * If False, return one list of records of all instruments,
              sorted by LOCAL_TIME.
            * If True, return a dictionary of {instrument_name: list of data},
              every list is sorted by LOCAL_TIME
        :param table: string, table name.
            * Default: tick data table.
        :param message: bool, whether to prompt out message on completion.
            * Default: True
        :return:
        """
        # flush md cache in Redis.
        self.redis_api.assert_db_index(AthenaConfig.ATHENA_REDIS_MD_DB_INDEX)
        self.redis_api.flush_db()

        # get market data from SQL server.
        rows = self.mssql_api.select_market_data(instruments_list, begin_time,
                                                 end_time, split_instruments,
                                                 table)
        digits = AthenaConfig.ATHENA_REDIS_MD_MAX_DIGITS
        # set directory
        data_dir = AthenaConfig.ATHENA_REDIS_MD_DIR

        # set headers
        if table == AthenaConfig.ATHENA_SQL_TABLE_NAME_TICK: # tick
            header = AthenaConfig.ATHENA_SQL_TABLE_HEADERS_TICK
        else: # transport bars
            header = AthenaConfig.ATHENA_SQL_TABLE_HEADERS_BAR

        # transport data
        counter = 0
        for record in rows:
            dict_record = dict(zip(header, record))
            # set the key: fill zeros before counter.
            # say, 2000 records, # 134 key is 'md_backtest:0134'
            data_key = data_dir + ':' + (digits - len(str(counter)))*'0' \
                       + str(counter)
            self.redis_api.set_dict(data_key, dict_record)
            counter += 1

        # add an auxiliary record that marks the end of history
        eof_key = data_dir + ':' + digits * '9'
        self.redis_api.set_dict(eof_key, {
            AthenaConfig.ATHENA_REDIS_MD_END_FLAG:
                AthenaConfig.ATHENA_REDIS_MD_END_FLAG})

        # message
        if message:
            print('[Transporter]: Historical data transported to Redis.')
            print('               * Instruments: {}'.format(
                str(instruments_list)))
            print('               * Time Range: {} - {}'.format(
                str(begin_time), str(end_time)))

