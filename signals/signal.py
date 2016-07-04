import json
from abc import ABCMeta, abstractmethod

from Athena.settings import AthenaConfig
from Athena.apis.database_api import RedisAPI

__author__ = 'zed'


class Signal(object):
    """

    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def __connect(self):
        """

        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def __subscribe_instruments(self):
        """

        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def on_market_data(self, data):
        """

        :param data:
        :return:
        """
        raise NotImplementedError


class SingleInstrumentSignal(Signal):
    """
    Implementation of Signal object that subscribe only one instrument.
    i.e. one market data ticker.
    """
    def __init__(self, instrument):
        """
        constructor.
        :param instrument: the market ticker that the signal listens to.
        """
        self.instrument = instrument
        self.digits = AthenaConfig.ATHENA_REDIS_MD_MAX_DIGITS
        self.counter = 0
        self.__connect()
        self.__subscribe_instruments()

    def __connect(self):
        """
        connect to two redis api. One is to listen to market data,
        another is to write calculate signal in redis.
        These two connections target to different data bases. Specifed
        :return:
        """
        # the connection that subscribes market data.
        self.md_api = RedisAPI(db=AthenaConfig.ATHENA_REDIS_MD_DB_INDEX)

        # the connection that publishes signal
        self.signal_api = RedisAPI(
            db=AthenaConfig.ATHENA_REDIS_SIGNAL_DB_INDEX)

    def __subscribe_instruments(self):
        """
        subscribe some instrument by listening to the channel in Redis.
        :return:
        """
        # create a sub.
        self.sub = self.md_api.connection.pubsub()
        # channel directory (set in MarketDataHandler
        channel = self.instrument
        self.sub.subscribe(channel)

    def start(self):
        """let signal start running."""
        for message in self.sub.listen():
            if message['type'] == 'message':
                str_data = message['data'].decode('utf-8')
                dict_data = json.loads(str_data)
                self.on_market_data(list(dict_data.values())[0])

    def on_market_data(self, data):
        """
        The logic that is being executed every time market data is received.
        :param data:
        :return:
        """
        pass


class NaiveSingleInstrumentSignal(SingleInstrumentSignal):
    """
    A testing implementation of single instrument signal.
    output 0 if the md is BID(SLD), 1 if ask (BOT).
    """
    def __init__(self, instrument):
        """
        constructor.
        """
        super(NaiveSingleInstrumentSignal, self).__init__(instrument)

    def on_market_data(self, data):
        """
        The logic that is being executed every time market data is received.
        Implemented as just printing the data out.
        :param data:
        :return:
        """
        distribute_key = self.instrument + '_naive:' \
                         + (self.digits - len(str(self.counter))) * '0' \
                         + str(self.counter)
        self.signal_api.set_dict(distribute_key, {'naive':'naive'})
        self.counter += 1

