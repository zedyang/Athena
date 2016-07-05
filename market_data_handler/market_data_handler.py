import time
import json
from abc import ABCMeta, abstractmethod

from Athena.settings import AthenaConfig
from Athena.apis.database_api import RedisAPI
from Athena.portfolio.position import PositionDirection

__author__ = 'zed'


class MarketDataHandler(object):
    """
    Market data handler distributes the market data for backtest's main loop.
    Essentially, it iterates the history stream and put the records of
    different instruments into different channels.

    Before starting distribution, the data must be loaded to Redis by
    transporter.

    The Market data handler also maintains a market snapshot in the form:
    {instrument: data} and one simulated clock, every time the market data
    record is streamed, these two objected are updated accordingly.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def __connect(self):
        """

        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def distribute_data(self):
        """

        :return:
        """
        raise NotImplementedError


class TickDataHandler(MarketDataHandler):
    """
    Inherited class of market data handler, implements method that handles
    tick data.
    """
    def __init__(self, instruments_list):
        """
        constructor.
        :param instruments_list: list of string (instruments)
        """
        # basic configurations
        self.history_dir = AthenaConfig.ATHENA_REDIS_MD_DIR
        self.instruments_list = instruments_list
        self.streaming_interval = 0.01 # in seconds
        # initialize a dictionary of counters of all instruments.
        self.counters = dict()
        for instrument in instruments_list:
            self.counters[instrument] = 0

        # make connection
        self.__connect()

        # length of history
        self.history_keys = self.md_api.get_keys(
            '{}:*'.format(self.history_dir))
        self.history_length = len(self.history_keys)
        self.digits = AthenaConfig.ATHENA_REDIS_MD_MAX_DIGITS

        # market (quote) snapshot
        self.market_prices = dict()
        for instrument in self.instruments_list:
            self.market_prices[instrument] = {
                PositionDirection.BOT: [None, None],
                # our purchase price (ask), [0] for quote, [1] for vol
                PositionDirection.SLD: [None, None]
                # our selling price (bid)
            }

        # current time
        self.current_time = None

    def __connect(self):
        """login to redis"""
        # the connection that subscribes market data.
        self.md_api = RedisAPI(db=AthenaConfig.ATHENA_REDIS_MD_DB_INDEX)

    def stream_bar_by_key(self, key):
        """
        :param key, string
        :return: flag, 1 = continue, 0 = break
        """
        # TODO This should be a private method (__*) after debugged.

        # fetch data
        data = self.md_api.get_dict(key)

        # end flag encountered
        if AthenaConfig.ATHENA_REDIS_MD_END_FLAG in data:
            print('[MD Handler]: Done, historical data is exhausted.')
            for instrument in self.instruments_list:
                eof_key = instrument + ':' + self.digits * '9'
                self.md_api.set_dict(eof_key, data)
            return 0
        # otherwise
        # get instrument name
        this_instrument = \
            data[AthenaConfig.ATHENA_SQL_TABLE_FIELD_INSTRUMENT]

        # set the key
        distribute_key = this_instrument + ':' + \
                         (self.digits - len(
                             str(self.counters[this_instrument]))) * '0' + \
                         str(self.counters[this_instrument])

        # publish the data dict by renaming the key.
        self.md_api.reset_key(key, distribute_key)

        # publish message
        message = json.dumps({distribute_key: data})

        self.md_api.connection.publish(
            channel=this_instrument,
            message=message
        )

        # update market snapshot and the clock
        self.current_time = \
            data[AthenaConfig.ATHENA_SQL_TABLE_FIELD_DATETIME]

        if data[AthenaConfig.ATHENA_SQL_TABLE_FIELD_SUBTYPE] == 'ASK':
            self.market_prices[this_instrument][PositionDirection.BOT][0] = \
                data[AthenaConfig.ATHENA_SQL_TABLE_FIELD_PRICE]
            self.market_prices[this_instrument][PositionDirection.BOT][1] = \
                data[AthenaConfig.ATHENA_SQL_TABLE_FIELD_VOLUME]
        else:
            self.market_prices[this_instrument][PositionDirection.SLD][0] = \
                data[AthenaConfig.ATHENA_SQL_TABLE_FIELD_PRICE]
            self.market_prices[this_instrument][PositionDirection.SLD][1] = \
                data[AthenaConfig.ATHENA_SQL_TABLE_FIELD_VOLUME]

        # increase counter
        self.counters[this_instrument] += 1
        return 1

    def distribute_data(self):
        """
        Distribute tick data
        :return:
        """
        for hist_key in self.history_keys:
            flag = self.stream_bar_by_key(hist_key)
            if flag == 0:
                break
            # sleep and continue.
            # time.sleep(self.streaming_interval)



