import unittest
from datetime import datetime

from Athena.settings import AthenaConfig
from Athena.apis.database_api import RedisAPI
from Athena.apis.mssql_api import SQLServerAPI
from Athena.market_data_handler.data_transporter import DataTransporter

__author__ = 'zed'


class TestTransporter(unittest.TestCase):

    """
    Test Redis API.
    """
    def setUp(self):
        """
        set up the redis api.
        :return:
        """
        self.redis_api = RedisAPI(db=0)
        self.mssql_api = SQLServerAPI()
        self.transporter = DataTransporter(self.mssql_api, self.redis_api)

    def test_transporting_data_bars(self):
        """
        test transporting data
        :return:
        """
        # set backtest config
        instruments_list = ['GC1 Comdty']
        begin_time = datetime(2015, 9, 21, 9, 0, 0)  # 2015-09-21 09:00:00
        end_time = datetime(2015, 9, 22, 14, 0, 0)  # 2015-09-22 14:00:00

        # begin transportation
        self.transporter.transport_data(
            instruments_list, begin_time, end_time,
            table=AthenaConfig.ATHENA_SQL_TABLE_NAME_BAR_1M)

    def test_transporting_data_ticks(self):
        """
        test transporting data
        :return:
        """
        # set backtest config
        # instruments_list = ['AUAM5']
        # begin_time = datetime(2015, 1, 1, 9, 0, 0)  # 2014-08-06 09:00:00
        # end_time = datetime(2015, 2, 1, 14, 0, 0)  # 2015-08-07 14:00:00
        instruments_list = ['au1606', 'Au(T+D)']
        begin_time = datetime(2014, 8, 6, 9, 0, 0)  # 2014-08-06 09:00:00
        end_time = datetime(2015, 8, 7, 14, 0, 0)  # 2015-08-07 14:00:00

        # begin transportation
        self.transporter.transport_data(
            instruments_list, begin_time, end_time)

if __name__ == '__main__':
    unittest.main()