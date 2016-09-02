import time
import pymssql
from datetime import datetime

from Athena.settings import AthenaConfig
from Athena.data_handler.redis_wrapper import RedisWrapper
from Athena.data_handler.clean_data import clean_hermes_md_data, \
    clean_hermes_kl_data

HTf, HKf = AthenaConfig.HermesTickFields, AthenaConfig.HermesKLineFields

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

        # open sql connection.
        self.__login()
        self.cursor = self.connection.cursor()

        # open connection to redis
        self.cache_wrapper = RedisWrapper(
            db=AthenaConfig.daily_migration_cache_db_index)
        self.hermes_wrapper = RedisWrapper(db=AthenaConfig.hermes_db_index)

        # table names
        self.md_table_name = None
        self.kl_table_name = None

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

    def __migrate_daily_cached_keys(self):
        """
        migrate redis cache to another db to prepare for transportation.
        :return:
        """
        # all keys to migrate
        keys_to_migrate = self.hermes_wrapper.get_keys('*')

        self.hermes_wrapper.migrate_keys(
            keys_list=keys_to_migrate,
            target_db=AthenaConfig.daily_migration_cache_db_index
        )

        print('[Redis]: Migrated keys to cache db.')

    def __make_daily_storage_md_table(self, table_name=None):
        """
        make sql tables to store daily tick data.
        :return:
        """
        # make table name
        if table_name:
            self.md_table_name = table_name
        else:
            self.md_table_name = \
                'md_' + datetime.today().date().strftime('%Y%m%d')

        # drop table query
        qry_clean_up_md = """
        IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
        DROP TABLE {table_name};
        """.format(
            table_name=self.md_table_name
        )

        # execute clean up queries
        self.cursor.execute(qry_clean_up_md)
        self.connection.commit()

        # make table query
        qry_make_md_table = """
        CREATE TABLE {table_name}
            (
                [RowId] INT PRIMARY KEY,
                [TradingDay] DATE,
                [ExUpdateTime] DATETIME2,
                [LocalUpdateTime] DATETIME2,
                [ExchangeID] VARCHAR(10),
                [Category] VARCHAR(10),
                [Symbol] VARCHAR(25),
                [LastPrice] FLOAT,
                [BidPrice1] FLOAT,
                [BidPrice2] FLOAT,
                [BidPrice3] FLOAT,
                [BidPrice4] FLOAT,
                [BidPrice5] FLOAT,
                [BidPrice6] FLOAT,
                [BidPrice7] FLOAT,
                [BidPrice8] FLOAT,
                [BidPrice9] FLOAT,
                [BidPrice10] FLOAT,
                [BidVolume1] INT,
                [BidVolume2] INT,
                [BidVolume3] INT,
                [BidVolume4] INT,
                [BidVolume5] INT,
                [BidVolume6] INT,
                [BidVolume7] INT,
                [BidVolume8] INT,
                [BidVolume9] INT,
                [BidVolume10] INT,
                [AskPrice1] FLOAT,
                [AskPrice2] FLOAT,
                [AskPrice3] FLOAT,
                [AskPrice4] FLOAT,
                [AskPrice5] FLOAT,
                [AskPrice6] FLOAT,
                [AskPrice7] FLOAT,
                [AskPrice8] FLOAT,
                [AskPrice9] FLOAT,
                [AskPrice10] FLOAT,
                [AskVolume1] INT,
                [AskVolume2] INT,
                [AskVolume3] INT,
                [AskVolume4] INT,
                [AskVolume5] INT,
                [AskVolume6] INT,
                [AskVolume7] INT,
                [AskVolume8] INT,
                [AskVolume9] INT,
                [AskVolume10] INT,
                [AveragePrice] FLOAT,
                [HighestPrice] FLOAT,
                [LowestPrice] FLOAT,
                [PreClosePrice] FLOAT,
                [OpenInterest] INT,
                [Volume] INT,
                [Turnover] FLOAT,
                [Rank] INT,
                [UniqueIndex] VARCHAR(255),
            );
        """.format(
            table_name=self.md_table_name
        )

        # execute make table queries
        self.cursor.execute(qry_make_md_table)
        self.connection.commit()

        print('[SQL Server]: SQL table {} created.'.format(
            self.md_table_name
        ))

    def __make_daily_storage_kl_table(self, table_name=None):
        """

        :return:
        """
        # make table name
        if table_name:
            self.kl_table_name = table_name
        else:
            self.kl_table_name = \
                'kl_' + datetime.today().date().strftime('%Y%m%d')
        # drop table query
        qry_clean_up_kl = """
        IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
        DROP TABLE {table_name};
        """.format(
            table_name=self.kl_table_name
        )

        # execute clean up queries
        self.cursor.execute(qry_clean_up_kl)
        self.connection.commit()

        # make table query
        qry_make_kl_table = """
        CREATE TABLE {table_name}
            (
                [RowId] INT PRIMARY KEY,
                [TradingDay] DATE,
                [ExUpdateTime] DATETIME2,
                [LocalUpdateTime] DATETIME2,
                [ExchangeID] VARCHAR(10),
                [Category] VARCHAR(10),
                [Symbol] VARCHAR(25),
                [TimeFrame] INT,
                [Open] FLOAT,
                [High] FLOAT,
                [Low] FLOAT,
                [Close] FLOAT,
                [Volume] INT,
                [Turnover] INT,
                [OpenInterest] INT,
                [Average] FLOAT,
                [TotalVolume] INT,
                [TotalTurnover] INT,
                [DayAveragePrice] FLOAT,
                [OpenTime] DATETIME2,
                [HighTime] DATETIME2,
                [LowTime] DATETIME2,
                [CloseTime] DATETIME2,
                [Rank] INT,
                [UniqueIndex] VARCHAR(255)
            );
        """.format(
            table_name=self.kl_table_name
        )

        self.cursor.execute(qry_make_kl_table)
        self.connection.commit()

        print('[SQL Server]: SQL table {} created.'.format(
            self.kl_table_name
        ))

    def __solidify_md_data(self):
        """
        transport redis data to sql.
        :return:
        """
        start_time = time.time()

        # get md keys
        md_keys = self.cache_wrapper.get_keys('md.*[:]*')
        N = len(md_keys)

        # sort keys.
        print('[Redis]: Sorting records according to temporal sequence.')
        md_key_localtime_tuples = []
        for k in md_keys:
            try:
                # get hash set
                row = self.cache_wrapper.get_dict(k)
            except UnicodeError:
                print('[Redis]: Unicode error at key {}.'.format(k))
                continue

            # append (key, local_time)
            md_key_localtime_tuples.append((k, int(row[HTf.local_time])))

            if not len(md_key_localtime_tuples) % 10000:
                print('[Redis]: Sorted {} %.'.format(
                    round(100 * len(md_key_localtime_tuples) / N, 2)
                ), flush=True)

        md_sorted_keys = sorted(
            md_key_localtime_tuples,
            key=lambda tup: tup[1]
        )

        print('[Redis]: Ready to transport {} records from redis'.format(
            len(md_sorted_keys)
        ))

        # begin iterating through keys and insert data into table.
        counter_md = 0
        for (k, l) in md_sorted_keys:
            try:
                # get hash set
                row = self.cache_wrapper.get_dict(k)
            except UnicodeError:
                print('[Redis]: Unicode error at key {}.'.format(k))
                continue

            # clean data
            cleaned_row = clean_hermes_md_data(row, is_hash_set=True)

            this_symbol = cleaned_row[HTf.contract]
            local_update_time = \
                cleaned_row[HTf.local_time].strftime(
                    AthenaConfig.sql_storage_dt_format)

            qry_insert_row = """
            INSERT INTO {table} VALUES
                (
                    {row_id},
                    '{day}',
                    '{ex_update_time}',
                    '{local_update_time}',
                    '{exchange}',
                    '{category}',
                    '{symbol}',
                    {last_price},
                    {bid_1},
                    {bid_2},
                    {bid_3},
                    {bid_4},
                    {bid_5},
                    {bid_6},
                    {bid_7},
                    {bid_8},
                    {bid_9},
                    {bid_10},
                    {bid_vol_1},
                    {bid_vol_2},
                    {bid_vol_3},
                    {bid_vol_4},
                    {bid_vol_5},
                    {bid_vol_6},
                    {bid_vol_7},
                    {bid_vol_8},
                    {bid_vol_9},
                    {bid_vol_10},
                    {ask_1},
                    {ask_2},
                    {ask_3},
                    {ask_4},
                    {ask_5},
                    {ask_6},
                    {ask_7},
                    {ask_8},
                    {ask_9},
                    {ask_10},
                    {ask_vol_1},
                    {ask_vol_2},
                    {ask_vol_3},
                    {ask_vol_4},
                    {ask_vol_5},
                    {ask_vol_6},
                    {ask_vol_7},
                    {ask_vol_8},
                    {ask_vol_9},
                    {ask_vol_10},
                    {avg_price},
                    {high_price},
                    {low_price},
                    {pre_close},
                    {open_int},
                    {volume},
                    {turnover},
                    {rank},
                    '{index}'
                )
            """ .format(
                table=self.md_table_name,
                row_id=counter_md,
                day=local_update_time,
                ex_update_time=cleaned_row[
                    HTf.ex_time].strftime(AthenaConfig.sql_storage_dt_format),
                local_update_time=local_update_time,
                exchange=AthenaConfig.hermes_exchange_mapping[
                    this_symbol],
                category=AthenaConfig.hermes_category_mapping[
                    this_symbol],
                symbol=this_symbol,
                last_price=cleaned_row[HTf.last_price],
                bid_1=cleaned_row[HTf.bid_1],
                bid_2=cleaned_row[HTf.bid_2],
                bid_3=cleaned_row[HTf.bid_3],
                bid_4=cleaned_row[HTf.bid_4],
                bid_5=cleaned_row[HTf.bid_5],
                bid_6=cleaned_row[HTf.bid_6],
                bid_7=cleaned_row[HTf.bid_7],
                bid_8=cleaned_row[HTf.bid_8],
                bid_9=cleaned_row[HTf.bid_9],
                bid_10=cleaned_row[HTf.bid_10],
                bid_vol_1=cleaned_row[HTf.bid_vol_1],
                bid_vol_2=cleaned_row[HTf.bid_vol_2],
                bid_vol_3=cleaned_row[HTf.bid_vol_3],
                bid_vol_4=cleaned_row[HTf.bid_vol_4],
                bid_vol_5=cleaned_row[HTf.bid_vol_5],
                bid_vol_6=cleaned_row[HTf.bid_vol_6],
                bid_vol_7=cleaned_row[HTf.bid_vol_7],
                bid_vol_8=cleaned_row[HTf.bid_vol_8],
                bid_vol_9=cleaned_row[HTf.bid_vol_9],
                bid_vol_10=cleaned_row[HTf.bid_vol_10],
                ask_1=cleaned_row[HTf.ask_1],
                ask_2=cleaned_row[HTf.ask_2],
                ask_3=cleaned_row[HTf.ask_3],
                ask_4=cleaned_row[HTf.ask_4],
                ask_5=cleaned_row[HTf.ask_5],
                ask_6=cleaned_row[HTf.ask_6],
                ask_7=cleaned_row[HTf.ask_7],
                ask_8=cleaned_row[HTf.ask_8],
                ask_9=cleaned_row[HTf.ask_9],
                ask_10=cleaned_row[HTf.ask_10],
                ask_vol_1=cleaned_row[HTf.ask_vol_1],
                ask_vol_2=cleaned_row[HTf.ask_vol_2],
                ask_vol_3=cleaned_row[HTf.ask_vol_3],
                ask_vol_4=cleaned_row[HTf.ask_vol_4],
                ask_vol_5=cleaned_row[HTf.ask_vol_5],
                ask_vol_6=cleaned_row[HTf.ask_vol_6],
                ask_vol_7=cleaned_row[HTf.ask_vol_7],
                ask_vol_8=cleaned_row[HTf.ask_vol_8],
                ask_vol_9=cleaned_row[HTf.ask_vol_9],
                ask_vol_10=cleaned_row[HTf.ask_vol_10],
                avg_price=cleaned_row[HTf.average_price],
                high_price=cleaned_row[HTf.high_price],
                low_price=cleaned_row[HTf.low_price],
                pre_close=cleaned_row[HTf.pre_close_price],
                open_int=cleaned_row[HTf.open_interest],
                volume=cleaned_row[HTf.volume],
                turnover=cleaned_row[HTf.turnover],
                rank=0,
                index=k.decode('utf-8')
            )

            # execute insert data queries
            self.cursor.execute(qry_insert_row)
            self.connection.commit()

            # increment to counter
            counter_md += 1

            if not counter_md % 10000:
                print('[SQL Server]: Finished {}/{}.'.format(
                    counter_md, len(md_sorted_keys))
                )

        end_time = time.time()
        print('[SQL Server]: Transportation finished. '
              'Spent {} seconds.'.format(
                end_time-start_time
              ))

    def __solidify_kl_data(self):
        """
        transport redis kline data to sql.
        :return:
        """
        start_time = time.time()

        # get md keys
        kl_keys = self.cache_wrapper.get_keys('kl.*[:]*')

        # sort keys.
        print('[Redis]: Sorting records according to temporal sequence.')
        kl_key_localtime_tuples = []
        for k in kl_keys:
            try:
                # get hash set
                row = self.cache_wrapper.get_dict(k)
            except UnicodeError:
                print('[Redis]: Unicode error at key {}.'.format(k))
                continue

            # append (key, local_time)
            kl_key_localtime_tuples.append((k, int(row[HKf.open_time])))

            if not len(kl_key_localtime_tuples) % 10000:
                print('[Redis]: Sorted {} %.'.format(
                    round(100 * len(kl_key_localtime_tuples) / len(kl_keys), 2)
                ), flush=True)

        kl_sorted_keys = sorted(
            kl_key_localtime_tuples,
            key=lambda tup: tup[1]
        )

        print('[Redis]: Ready to transport {} records from redis'.format(
            len(kl_sorted_keys)
        ))

        # begin iterating through keys and insert data into table.
        counter_kl = 0
        for (k, l) in kl_sorted_keys:
            try:
                # get hash set
                row = self.cache_wrapper.get_dict(k)
            except UnicodeError:
                print('[Redis]: Unicode error at key {}.'.format(k))
                continue

            try:
                # clean data
                cleaned_row = clean_hermes_kl_data(row, is_hash_set=True)
            except ValueError:
                print('[Redis]: Illegal value at key {}.'.format(k))
                continue
            except OSError:
                print('[Redis]: Illegal value at key {}.'.format(k))
                continue

            this_symbol = cleaned_row[HKf.contract]
            local_update_time = \
                cleaned_row[HKf.local_time].strftime(
                    AthenaConfig.sql_storage_dt_format)

            qry_insert_row = """
            INSERT INTO {table} VALUES
                (
                    {row_id},
                    '{day}',
                    '{ex_update_time}',
                    '{local_update_time}',
                    '{exchange}',
                    '{category}',
                    '{symbol}',
                    {duration},
                    {open_price},
                    {high_price},
                    {low_price},
                    {close_price},
                    {volume},
                    {turnover},
                    {open_int},
                    {average_price},
                    {tot_volume},
                    {tot_turnover},
                    {day_average_price},
                    '{open_time}',
                    '{high_time}',
                    '{low_time}',
                    '{close_time}',
                    {rank},
                    '{index}'
                )
            """ .format(
                table=self.kl_table_name,
                row_id=counter_kl,
                day=local_update_time,
                ex_update_time=cleaned_row[
                    HKf.ex_time].strftime(AthenaConfig.sql_storage_dt_format),
                local_update_time=local_update_time,
                exchange=AthenaConfig.hermes_exchange_mapping[
                    this_symbol],
                category=AthenaConfig.hermes_category_mapping[
                    this_symbol],
                symbol=this_symbol,
                duration=cleaned_row[HKf.duration],
                open_price=cleaned_row[HKf.open_price],
                high_price=cleaned_row[HKf.high_price],
                low_price=cleaned_row[HKf.low_price],
                close_price=cleaned_row[HKf.close_price],
                volume=cleaned_row[HKf.volume],
                turnover=cleaned_row[HKf.turnover],
                open_int=cleaned_row[HKf.open_interest],
                average_price=cleaned_row[HKf.average_price],
                tot_volume=cleaned_row[HKf.total_volume],
                tot_turnover=cleaned_row[HKf.total_turnover],
                day_average_price=0,
                open_time=cleaned_row[
                    HKf.open_time].strftime(
                    AthenaConfig.sql_storage_dt_format),
                high_time=cleaned_row[
                    HKf.high_time].strftime(
                    AthenaConfig.sql_storage_dt_format),
                low_time=cleaned_row[
                    HKf.low_time].strftime(
                    AthenaConfig.sql_storage_dt_format),
                close_time=cleaned_row[
                    HKf.close_time].strftime(
                    AthenaConfig.sql_storage_dt_format),
                rank=0,
                index=k.decode('utf-8')
            )

            # execute insert data queries
            self.cursor.execute(qry_insert_row)
            self.connection.commit()

            # increment to counter
            counter_kl += 1

            if not counter_kl % 10000:
                print('[SQL Server]: Finished {}/{}.'.format(
                    counter_kl, len(kl_sorted_keys))
                )

        end_time = time.time()
        print('[SQL Server]: Transportation finished. '
              'Spent {} seconds.'.format(
                end_time-start_time)
        )

    def select_hist_data(self, symbols_list, begin_time, end_time, table,
                         split_instruments=False):
        """

        :param symbols_list:
        :param begin_time:
        :param end_time:
        :param split_instruments:
        :param table:
        :return:
        """
        # convert parameters to SQL command string.
        symbols_string = "('" + "','".join(symbols_list) + "')"
        # pay attention to quotation marks!
        #
        begin_time_str = "'" + begin_time.strftime(
            AthenaConfig.dt_format) + "'"
        end_time_str = "'" + end_time.strftime(
            AthenaConfig.dt_format) + "'"

        # execute SQL command.
        self.cursor.execute("""
                SELECT * FROM {table}
                WHERE [Symbol]
                IN {symbols_list}
                AND [LocalUpdateTime] >= {begin_time}
                AND [LocalUpdateTime] <= {end_time}
                ORDER BY [LocalUpdateTime]""".format(
            table=table,
            symbol=HKf.contract,
            symbols_list=symbols_string,
            local_update_time=HKf.local_time,
            begin_time=begin_time_str,
            end_time=end_time_str
        ))
        rows = [row for row in self.cursor]
        rows = sorted(
            rows,
            key=lambda tup: tup[0]
        )
        return rows

    def migrate_data(self):
        """
        daily migration to sql server.
        :return:
        """
        # migrate keys
        self.__migrate_daily_cached_keys()

        #  solidify md
        self.__make_daily_storage_md_table()
        self.__solidify_md_data()

        # solidify k lines
        self.__make_daily_storage_kl_table()
        self.__solidify_kl_data()

        # flush cached db
        self.cache_wrapper.flush_db()

    def logout(self):
        """ Log out from server."""
        if self.connection:
            self.connection.close()

