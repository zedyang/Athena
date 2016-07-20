import pymssql
import redis
from datetime import datetime

from Athena.settings import AthenaConfig
from Athena.data_handler.redis_wrapper import RedisWrapper
from Athena.utils import append_digits_suffix_for_redis_key

__author__ = 'zed'


class SQLWrapper(object):
    """
    The implementation of MS SQL api. Each instance will maintain a connection
    to SQL server.

    The interaction between Athena and SQL server is mainly one-batch retrieve
    of historical (large) market data set.
    There is no (currently) design in which Athena continuously insert or
    obtain records into SQL server.

    Therefore, unlike redis server, we hope to open only one connection to
    SQL server. Every module that interacts with SQL server should preserve
    a reference to the (only) instance.
    """

    def __init__(self):
        """
        Constructor.
        SQL connection configs are imported from AthenaConfig class.
        """
        self.host_name = AthenaConfig.sql_host
        self.port = AthenaConfig.sql_port
        self.usr_name = AthenaConfig.sql_usr
        self.pwd = AthenaConfig.sql_pwd
        self.initial_db = AthenaConfig.sql_historical_db

        # open connection.
        self.__login()
        self.cursor = self.connection.cursor()

        # header names
        self.tick_headers = AthenaConfig.sql_tick_headers

        self.instrument_field = \
            AthenaConfig.sql_instrument_field
        self.datetime_field = \
            AthenaConfig.sql_local_dt_field

        # open connection to redis
        self.__connect_redis()

    def __login(self):
        """ login to SQL server."""
        try:
            self.connection = pymssql.connect(
                server=self.host_name,
                user=self.usr_name,
                password=self.pwd,
                port=self.port,
                database=self.initial_db
            )
            print('[SQL Server]: Connected to SQL server.')
        except pymssql.Error:
            print('<Error>[SQL Server]: Could not connect to SQL server.')

    def __connect_redis(self):
        """make a redis connection"""
        self.redis = RedisWrapper(db=AthenaConfig.historical_md_db_index)

    def select_hist_data(self, instruments_list, begin_time, end_time,
                         split_instruments=False,
                         table=AthenaConfig.sql_test_table):
        """
        select market data from SQL server with specified instruments list
        and time range.

        :param instruments_list: list of strings,
            the instruments to be selected.
        :param begin_time: datetime.datetime object, beginning time.
        :param end_time: datetime.datetime object ending time.
        :param split_instruments: bool, whether to split the instruments.
            * Default = False
            * If False, return one list of records of all instruments,
              sorted by datetime field.
            * If True, return a dictionary of {instrument_name: list of data},
              every list is sorted by the datetime field.
        :param table: string, table name.
            * Default: tick data table.
        :return:
        """
        # convert parameters to SQL command string.
        instruments_string = "('" + "','".join(instruments_list) + "')"
        # pay attention to quotation marks!
        #
        begin_time_str = "'" + begin_time.strftime(
            AthenaConfig.dt_format) + "'"
        end_time_str = "'" + end_time.strftime(
            AthenaConfig.dt_format) + "'"

        # execute SQL command.
        self.cursor.execute("""
            SELECT * FROM {table}
            WHERE {instrument_field} IN {instruments_list}
            AND {time_field} >= {begin_time} AND {time_field} <= {end_time}
            ORDER BY {time_field}""".format(
            table=table,
            instrument_field=self.instrument_field,
            instruments_list=instruments_string,
            time_field=self.datetime_field,
            begin_time=begin_time_str,
            end_time=end_time_str
        ))
        rows = [row for row in self.cursor]
        return rows

    def transport_hist_data(self, instruments_list, begin_time, end_time,
                            split_instruments=False,
                            table=AthenaConfig.sql_test_table,
                            message=True):
        """
        transport data to redis db. The parameters list is a replicate
        of select_hist_data.
        :param instruments_list:
        :param begin_time:
        :param end_time:
        :param split_instruments:
        :param table:
        :param message:
        :return:
        """
        # flush redis cache
        self.redis.flush_db()

        # get market data from SQL server.
        rows = self.select_hist_data(instruments_list, begin_time,
                                     end_time, split_instruments,
                                     table)
        # set directory
        data_dir = AthenaConfig.redis_md_dir

        # set headers according to tables.
        header = AthenaConfig.sql_tick_headers

        # transport data
        counter = 0
        for record in rows:
            dict_record = dict(zip(header, record))
            key_in_redis = append_digits_suffix_for_redis_key(
                data_dir, counter)
            self.redis.set_dict(key_in_redis, dict_record)
            counter += 1

        # add an auxiliary record that marks the end of history
        eof_key = data_dir + ':' + AthenaConfig.redis_key_max_digits * '9'
        self.redis.set_dict(eof_key, {
            AthenaConfig.redis_md_end_flag:
                AthenaConfig.redis_md_end_flag})

        # message
        if message:
            print('[SQL Wrapper]: Historical data transported to Redis.')
            print('               * Instruments: {}'.format(
                str(instruments_list)))
            print('               * Time Range: {} - {}'.format(
                str(begin_time), str(end_time)))

    def logout(self):
        """ Log out from server."""
        if self.connection:
            self.connection.close()