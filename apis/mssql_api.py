import pymssql
from datetime import datetime

from Athena.utils import singleton
from Athena.settings import AthenaConfig
from Athena.apis.database_api import DatabaseAPI

__author__ = 'zed'


class SQLServerAPI(DatabaseAPI):
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
        self.host_name = AthenaConfig.ATHENA_SQL_HOST_NAME
        self.port = AthenaConfig.ATHENA_SQL_PORT
        self.usr_name = AthenaConfig.ATHENA_SQL_LOGIN_NAME
        self.pwd = AthenaConfig.ATHENA_SQL_LOGIN_PWD
        self.initial_db = AthenaConfig.ATHENA_SQL_DB_NAME

        # open connection.
        self.__login()
        self.cursor = self.connection.cursor()

        # header names
        self.headers_tick = AthenaConfig.ATHENA_SQL_TABLE_HEADERS_TICK
        self.headers_bar = AthenaConfig.ATHENA_SQL_TABLE_HEADERS_BAR
        self.colname_instrument = \
            AthenaConfig.ATHENA_SQL_TABLE_FIELD_INSTRUMENT
        self.colname_datetime = \
            AthenaConfig.ATHENA_SQL_TABLE_FIELD_DATETIME

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
            print('[SQL Server API]: Connected to SQL server.')
        except pymssql.Error:
            print('<Error>[SQL Server API]: Could not connect to SQL server.')

    def select_market_data(self, instruments_list, begin_time, end_time,
                           split_instruments=False,
                           table=AthenaConfig.ATHENA_SQL_TABLE_NAME_TICK):
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
              sorted by LOCAL_TIME.
            * If True, return a dictionary of {instrument_name: list of data},
              every list is sorted by LOCAL_TIME
        :param table: string, table name.
            * Default: tick data table.
        :return:
        """
        # convert parameters to SQL command string.
        instruments_string = "('" + "','".join(instruments_list) + "')"
        # pay attention to quotation marks!
        begin_time_str = "'" + begin_time.strftime(
            AthenaConfig.ATHENA_SQL_DT_FORMAT) + "'"
        end_time_str = "'" + end_time.strftime(
            AthenaConfig.ATHENA_SQL_DT_FORMAT) + "'"

        # execute SQL command.
        self.cursor.execute("""
            SELECT * FROM {table}
            WHERE {instrument_field} IN {instruments_list}
            AND {time_field} >= {begin_time} AND {time_field} <= {end_time}
            ORDER BY {time_field}""".format(
            table=table,
            instrument_field=AthenaConfig.ATHENA_SQL_TABLE_FIELD_INSTRUMENT,
            instruments_list=instruments_string,
            time_field=AthenaConfig.ATHENA_SQL_TABLE_FIELD_DATETIME,
            begin_time=begin_time_str,
            end_time=end_time_str
        ))
        rows = [row for row in self.cursor]
        return rows

    def logout(self):
        """ Log out from server."""
        if self.connection:
            self.connection.close()
