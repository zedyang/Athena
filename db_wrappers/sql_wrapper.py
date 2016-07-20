import time
import pymssql
from datetime import datetime

from Athena.settings import AthenaConfig
from Athena.db_wrappers.redis_wrapper import RedisWrapper
from Athena.data_handler.clean_data import clean_hermes_data
from Athena.utils import append_digits_suffix_for_redis_key

Tf, Kf = AthenaConfig.TickFields, AthenaConfig.KLineFields
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

    def __make_daily_storage_md_table(self):
        """
        make sql tables to store daily tick data.
        :return:
        """
        # make table name
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
                [ExUpdateTime] TIME,
                [LocalUpdateTime] TIME,
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

    def __make_daily_storage_kl_table(self):
        """

        :return:
        """
        # make table name
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
                [ExUpdateTime] TIME,
                [LocalUpdateTime] TIME,
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
                [OpenTime] DATETIME,
                [HighTime] DATETIME,
                [LowTime] DATETIME,
                [CloseTime] DATETIME,
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

                # get instrument name from key.
                parsed_key = k.decode('utf-8').split(':')[0].split('.')
                if len(parsed_key) == 3:
                    this_symbol = parsed_key[-1]
                elif len(parsed_key) == 4:  # Au99.99, an extra '.'
                    this_symbol = '.'.join([parsed_key[-2], parsed_key[-1]])
                else:
                    this_symbol = parsed_key[-1]
            except UnicodeError:
                print('[Redis]: Unicode error at key {}.'.format(k))
                continue

            # clean data
            cleaned_row = clean_hermes_data(
                row, is_hash_set=True, this_contract=this_symbol
            )

            local_update_time = \
                cleaned_row[Tf.local_time].strftime(
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
                    Tf.ex_time].strftime(AthenaConfig.sql_storage_dt_format),
                local_update_time=local_update_time,
                exchange=AthenaConfig.hermes_exchange_mapping[
                    this_symbol],
                category=AthenaConfig.hermes_category_mapping[
                    this_symbol],
                symbol=this_symbol,
                last_price=cleaned_row[Tf.last_price],
                bid_1=cleaned_row[Tf.bid],
                bid_2=0,
                bid_3=0,
                bid_4=0,
                bid_5=0,
                bid_6=0,
                bid_7=0,
                bid_8=0,
                bid_9=0,
                bid_10=0,
                bid_vol_1=cleaned_row[Tf.bid_vol],
                bid_vol_2=0,
                bid_vol_3=0,
                bid_vol_4=0,
                bid_vol_5=0,
                bid_vol_6=0,
                bid_vol_7=0,
                bid_vol_8=0,
                bid_vol_9=0,
                bid_vol_10=0,
                ask_1=cleaned_row[Tf.ask],
                ask_2=0,
                ask_3=0,
                ask_4=0,
                ask_5=0,
                ask_6=0,
                ask_7=0,
                ask_8=0,
                ask_9=0,
                ask_10=0,
                ask_vol_1=cleaned_row[Tf.ask_vol],
                ask_vol_2=0,
                ask_vol_3=0,
                ask_vol_4=0,
                ask_vol_5=0,
                ask_vol_6=0,
                ask_vol_7=0,
                ask_vol_8=0,
                ask_vol_9=0,
                ask_vol_10=0,
                avg_price=0,
                high_price=0,
                low_price=0,
                pre_close=0,
                open_int=cleaned_row[Tf.open_int],
                volume=cleaned_row[Tf.volume],
                turnover=0,
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

                # get instrument name from key.
                parsed_key = k.decode('utf-8').split(':')[0].split('.')
                if len(parsed_key) == 4:
                    this_symbol = parsed_key[-2]
                elif len(parsed_key) == 5:  # Au99.99, an extra '.'
                    this_symbol = '.'.join([parsed_key[-3], parsed_key[-2]])
                else:
                    this_symbol = parsed_key[-2]
            except UnicodeError:
                print('[Redis]: Unicode error at key {}.'.format(k))
                continue

            try:
                # clean data
                cleaned_row = clean_hermes_data(
                    row, is_hash_set=True, this_contract=this_symbol
                )
            except ValueError:
                print('[Redis]: Illegal value at key {}.'.format(k))
                continue
            except OSError:
                print('[Redis]: Illegal value at key {}.'.format(k))
                continue

            local_update_time = \
                cleaned_row[Kf.end_time].strftime(
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
                    Kf.end_time].strftime(AthenaConfig.sql_storage_dt_format),
                local_update_time=local_update_time,
                exchange=AthenaConfig.hermes_exchange_mapping[
                    this_symbol],
                category=AthenaConfig.hermes_category_mapping[
                    this_symbol],
                symbol=this_symbol,
                duration=cleaned_row[Kf.duration],
                open_price=cleaned_row[Kf.open_price],
                high_price=cleaned_row[Kf.high_price],
                low_price=cleaned_row[Kf.low_price],
                close_price=cleaned_row[Kf.close_price],
                volume=cleaned_row[Kf.volume],
                turnover=0,
                open_int=cleaned_row[Tf.open_int],
                average_price=0,
                tot_volume=0,
                tot_turnover=0,
                day_average_price=0,
                open_time='',
                high_time='',
                low_time='',
                close_time='',
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
            end_time-start_time
        ))

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

