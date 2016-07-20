import pymssql
from datetime import datetime

from Athena.db_wrappers.redis_wrapper import RedisWrapper
from Athena.settings import AthenaConfig
from Athena.data_handler.clean_data import clean_hermes_data
Tf = AthenaConfig.TickFields

if __name__ == '__main__':
    r = RedisWrapper(db=AthenaConfig.hermes_db_index)
    #md_keys = r.get_keys('md.*')
    #print(len(md_keys))
    #md_sorted_keys = sorted(md_keys,
    #                        key=lambda k: k.decode('utf-8').split(':')[-1])
    #print(md_sorted_keys[1:1000])

    host_name = AthenaConfig.sql_host
    port = AthenaConfig.sql_port
    usr_name = AthenaConfig.sql_usr
    pwd = AthenaConfig.sql_pwd
    initial_db = AthenaConfig.sql_historical_db


    connection = pymssql.connect(
        server=host_name,
        user=usr_name,
        password=pwd,
        port=port,
        database=initial_db
    )
    print('[SQL Server]: Connected to SQL server.')

    cursor = connection.cursor()

    qry_clean_up_md = """
    IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
    DROP TABLE {table_name};
    """.format(
        table_name='test'
    )

    cursor.execute(qry_clean_up_md)
    connection.commit()

    qry1 = """
    CREATE TABLE md
    (
    RowId INT PRIMARY KEY ,
    TradingDay DATE,
    UpdateTime TIME,
    ExUpdateTime DATETIME,
    LocalUpdateTime DATETIME,
    Category VARCHAR(10),
    Symbol VARCHAR(25),
    LastPrice FLOAT,
    BidPrice1 FLOAT,
    BidVolume1 INT,
    AskPrice1 FLOAT,
    AskVolume INT,
    AveragePrice FLOAT,
    HighestPrice FLOAT,
    LowestPrice FLOAT,
    PreClosePrice FLOAT,
    OpenInterest INT,
    Volume INT,
    Turnover FLOAT,
    UniqueIndex VARCHAR(255),
    Rank INT
    )
    """

    k = 'md.nanhua.GC1608:131133658512000114'
    dd = r.get_dict(b'md.nanhua.GC1608:131133658512000114')
    cd = clean_hermes_data(dd, is_hash_set=True, this_contract='GC1608')
    print(cd)

    qry2 = """
    CREATE TABLE test
    (
    UpdateTime TIME,
    )
    """
    cursor.execute(qry2)
    connection.commit()

    qry3 = """
    INSERT INTO test VALUES ('{}')
    """.format(cd[Tf.local_time].strftime(AthenaConfig.sql_storage_dt_format))

    cursor.execute(qry3)
    connection.commit()






