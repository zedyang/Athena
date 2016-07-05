from datetime import datetime

from Athena.market_data_handler.market_data_handler import TickDataHandler
from Athena.market_data_handler.data_transporter import DataTransporter
from Athena.apis.database_api import RedisAPI
from Athena.apis.mssql_api import SQLServerAPI

__author__ = 'zed'


def transport_history_data_pub():
    """
    Test the naive signal.
    :return:
    """
    redis_api = RedisAPI(db=0)
    mssql_api = SQLServerAPI()
    transporter = DataTransporter(mssql_api, redis_api)

    redis_api.flush_all()

    begin_time = datetime(2014, 8, 6, 9, 0, 0)
    end_time = datetime(2015, 8, 7, 14, 0, 0)

    # begin transport
    transporter.transport_data(
        ['au1606'], begin_time, end_time
    )


if __name__ == '__main__':
    transport_history_data_pub()

    # begin publishing
    instruments_list = ['au1606', 'Au1606']
    data_handler = TickDataHandler(instruments_list)
    data_handler.distribute_data()