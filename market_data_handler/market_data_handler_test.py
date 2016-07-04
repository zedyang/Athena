import unittest
from datetime import datetime

from Athena.settings import AthenaConfig
from Athena.portfolio.position import PositionDirection
from Athena.apis.database_api import RedisAPI
from Athena.apis.mssql_api import SQLServerAPI
from Athena.market_data_handler.data_transporter import DataTransporter
from Athena.market_data_handler.market_data_handler import TickDataHandler

__author__ = 'zed'

test_keys = [
    'md_backtest:000', 'md_backtest:001', 'md_backtest:002',
    'md_backtest:003', 'md_backtest:004', 'md_backtest:005',
    'md_backtest:006', 'md_backtest:007', 'md_backtest:999'
]

test_keys_longer = [
    'md_backtest:000', 'md_backtest:001', 'md_backtest:002',
    'md_backtest:003', 'md_backtest:004', 'md_backtest:005',
    'md_backtest:006', 'md_backtest:007', 'md_backtest:008',
    'md_backtest:009', 'md_backtest:010', 'md_backtest:011',
    'md_backtest:999'
]

test_data = [
    {
        'md_backtest:000':
            {
                'SECURITY': 'au1606',
                'LOCAL_TIME': 0,
                'MD_SUBTYPE': 'BID',
                'PRICE': '10',
                'VOLUME': '105'
            }
    },
    {
        'md_backtest:001':
            {
                'SECURITY': 'au1606',
                'LOCAL_TIME': 1,
                'MD_SUBTYPE': 'ASK',
                'PRICE': '10.5',
                'VOLUME': '100'
            }
    },
    {
        'md_backtest:002':
            {
                'SECURITY': 'Au(T+D)',
                'LOCAL_TIME': 2,
                'MD_SUBTYPE': 'BID',
                'PRICE': '20',
                'VOLUME': '200'
            }
    },
    {
        'md_backtest:003':
            {
                'SECURITY': 'au1606',
                'LOCAL_TIME': 3,
                'MD_SUBTYPE': 'BID',
                'PRICE': '10.3',
                'VOLUME': '90'
            }
    },
    {
        'md_backtest:004':
            {
                'SECURITY': 'Au(T+D)',
                'LOCAL_TIME': 4,
                'MD_SUBTYPE': 'ASK',
                'PRICE': '21',
                'VOLUME': '195'
            }
    },
    {
        'md_backtest:005':
            {
                'SECURITY': 'au1606',
                'LOCAL_TIME': 5,
                'MD_SUBTYPE': 'ASK',
                'PRICE': '10.8',
                'VOLUME': '100'
            }
    },
    {
        'md_backtest:006':
            {
                'SECURITY': 'Au(T+D)',
                'LOCAL_TIME': 6,
                'MD_SUBTYPE': 'BID',
                'PRICE': '20.3',
                'VOLUME': '203'
            }
    },
    {
        'md_backtest:007':
            {
                'SECURITY': 'Au(T+D)',
                'LOCAL_TIME': 7,
                'MD_SUBTYPE': 'ASK',
                'PRICE': '21.2',
                'VOLUME': '197'
            }
    },
    {
        'md_backtest:999':
            {
                AthenaConfig.ATHENA_REDIS_MD_END_FLAG:
                    AthenaConfig.ATHENA_REDIS_MD_END_FLAG
            }
    },
]

test_data_longer_single_instrument = [
    {
        'md_backtest:000':
            {
                'SECURITY': 'au1606',
                'LOCAL_TIME': '2015-10-08 09:00:00',
                'MD_SUBTYPE': 'BID',
                'PRICE': '10',
                'VOLUME': '105'
            }
    },
    {
        'md_backtest:001':
            {
                'SECURITY': 'au1606',
                'LOCAL_TIME': '2015-10-08 09:00:00',
                'MD_SUBTYPE': 'ASK',
                'PRICE': '10.5',
                'VOLUME': '100'
            }
    },
    {
        'md_backtest:002':
            {
                'SECURITY': 'au1606',
                'LOCAL_TIME': '2015-10-08 09:00:26',
                'MD_SUBTYPE': 'BID',
                'PRICE': '10.3',
                'VOLUME': '130'
            }
    },
    {
        'md_backtest:003':
            {
                'SECURITY': 'au1606',
                'LOCAL_TIME': '2015-10-08 09:00:26',
                'MD_SUBTYPE': 'ASK',
                'PRICE': '10.8',
                'VOLUME': '90'
            }
    },
    {
        'md_backtest:004':
            {
                'SECURITY': 'au1606',
                'LOCAL_TIME': '2015-10-08 09:01:02',
                'MD_SUBTYPE': 'BID',
                'PRICE': '10.2',
                'VOLUME': '90'
            }
    },
    {
        'md_backtest:005':
            {
                'SECURITY': 'au1606',
                'LOCAL_TIME': '2015-10-08 09:01:02',
                'MD_SUBTYPE': 'ASK',
                'PRICE': '10.7',
                'VOLUME': '95'
            }
    },
    {
        'md_backtest:006':
            {
                'SECURITY': 'au1606',
                'LOCAL_TIME': '2015-10-08 09:01:33',
                'MD_SUBTYPE': 'BID',
                'PRICE': '10.8',
                'VOLUME': '120'
            }
    },
    {
        'md_backtest:007':
            {
                'SECURITY': 'au1606',
                'LOCAL_TIME': '2015-10-08 09:01:33',
                'MD_SUBTYPE': 'ASK',
                'PRICE': '11.3',
                'VOLUME': '115'
            }
    },
    {
        'md_backtest:008':
            {
                'SECURITY': 'au1606',
                'LOCAL_TIME': '2015-10-08 09:01:50',
                'MD_SUBTYPE': 'BID',
                'PRICE': '10.6',
                'VOLUME': '80'
            }
    },
    {
        'md_backtest:009':
            {
                'SECURITY': 'au1606',
                'LOCAL_TIME': '2015-10-08 09:01:50',
                'MD_SUBTYPE': 'ASK',
                'PRICE': '11.1',
                'VOLUME': '85'
            }
    },
    {
        'md_backtest:010':
            {
                'SECURITY': 'au1606',
                'LOCAL_TIME': '2015-10-08 09:02:03',
                'MD_SUBTYPE': 'BID',
                'PRICE': '10',
                'VOLUME': '80'
            }
    },
    {
        'md_backtest:011':
            {
                'SECURITY': 'au1606',
                'LOCAL_TIME': '2015-10-08 09:02:03',
                'MD_SUBTYPE': 'ASK',
                'PRICE': '10.5',
                'VOLUME': '85'
            }
    },
    {
        'md_backtest:999':
            {
                AthenaConfig.ATHENA_REDIS_MD_END_FLAG:
                    AthenaConfig.ATHENA_REDIS_MD_END_FLAG
            }
    },
]


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

        self.instruments_list = ['Au(T+D)', 'Au1606', 'au1606']
        self.data_handler = TickDataHandler(self.instruments_list)

    def test_data_stream_1(self):
        """

        :return:
        """
        # return
        self.redis_api.flush_all()
        for d in test_data:
            self.redis_api.set_dict(list(d.keys())[0], list(d.values())[0])

        hist_keys = ['md_backtest:000','md_backtest:001','md_backtest:002',
                     'md_backtest:003','md_backtest:004','md_backtest:005',
                     'md_backtest:006','md_backtest:007','md_backtest:999']

        # streaming one by one
        # 000
        self.data_handler.stream_bar_by_key('md_backtest:000')
        mkt_prices = self.data_handler.market_prices
        curr_time = self.data_handler.current_time
        self.assertEqual(curr_time, '0')
        self.assertEqual(mkt_prices['Au(T+D)'][PositionDirection.BOT],
                         [None, None])
        self.assertEqual(mkt_prices['Au(T+D)'][PositionDirection.SLD],
                         [None, None])
        self.assertEqual(mkt_prices['au1606'][PositionDirection.SLD],
                         ['10', '105'])

        # 001
        self.data_handler.stream_bar_by_key('md_backtest:001')
        mkt_prices = self.data_handler.market_prices
        curr_time = self.data_handler.current_time
        self.assertEqual(curr_time, '1')
        self.assertEqual(mkt_prices['au1606'][PositionDirection.SLD],
                         ['10', '105'])
        self.assertEqual(mkt_prices['au1606'][PositionDirection.BOT],
                         ['10.5', '100'])

        # 002
        self.data_handler.stream_bar_by_key('md_backtest:002')
        # 003
        self.data_handler.stream_bar_by_key('md_backtest:003')
        # 004
        self.data_handler.stream_bar_by_key('md_backtest:004')
        mkt_prices = self.data_handler.market_prices
        curr_time = self.data_handler.current_time
        self.assertEqual(curr_time, '4')
        self.assertEqual(mkt_prices['au1606'][PositionDirection.SLD],
                         ['10.3', '90'])
        self.assertEqual(mkt_prices['au1606'][PositionDirection.BOT],
                         ['10.5', '100'])
        self.assertEqual(mkt_prices['Au(T+D)'][PositionDirection.SLD],
                         ['20', '200'])
        self.assertEqual(mkt_prices['Au(T+D)'][PositionDirection.BOT],
                         ['21', '195'])

        # 005, 6, 7, 999
        self.data_handler.stream_bar_by_key('md_backtest:005')
        self.data_handler.stream_bar_by_key('md_backtest:006')
        self.data_handler.stream_bar_by_key('md_backtest:007')
        self.data_handler.stream_bar_by_key('md_backtest:999')
        # print(mkt_prices)

    def test_data_stream_2(self):
        """

        :return:
        """
        return
        instruments_list = ['au1606', 'Au(T+D)']
        begin_time = datetime(2014, 8, 6, 9, 0, 0)  # 2014-08-06 09:00:00
        end_time = datetime(2015, 8, 7, 14, 0, 0)  # 2015-08-07 14:00:00

        # begin transportation
        self.transporter.transport_data(
            instruments_list, begin_time, end_time)

        # begin streaming
        self.data_handler.distribute_data()


if __name__ == '__main__':
    unittest.main()