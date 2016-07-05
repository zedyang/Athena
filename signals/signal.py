import json
from abc import ABCMeta, abstractmethod

from Athena.settings import AthenaConfig
from Athena.apis.database_api import RedisAPI

__author__ = 'zed'


class Signal(object):
    """
    The abstract signal class, provides interfaces to all derived signal
    types. A signal will open one or more connections to the redis database.

    It subscribes to some channels (md, other signal or portfolio)
    according to its subtype. For example, a SingleInstrumentSignal will
    only listens to one channel: market data of the instrument it is tracking.

    when receiving a message on the listening channel, it execute the main
    logic in on_message() method. Signal can have more than one logical call
    backs according to the type/channel that the message is belongs to.

    Concrete Signal classes should also implement key_in_redis() method to
    map to a key so as to write record into redis.
    publish_message method is to write this hash set and publish message in
    its own channel that other message/strategy might listens to.
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
    def key_in_redis(self):
        """

        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def publish_message(self, dict_message_data):
        """

        :param dict_message_data:
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def on_message(self, message):
        """

        :param message:
        :return:
        """
        raise NotImplementedError


class SingleInstrumentSignal(Signal):
    """
    Implementation of Signal object that subscribe only one instrument.
    i.e. one market data ticker.
    The pub channel is signal_name + '_' + 'instrument'
    and the sub channel is just instrument.

    The class inherits SingleInstrument signal must reset signal_name
    and pub_channel.
    """

    def __init__(self, instrument):
        """
        constructor.
        :param instrument: the market ticker that the signal listens to.
        """
        self.signal_name = 'abstract_single'
        self.instrument = instrument

        self.digits = AthenaConfig.ATHENA_REDIS_MD_MAX_DIGITS
        self.counter = 0
        self.__connect()
        self.__subscribe_instruments()

        # set the name of pub channels
        self.pub_channel = self.signal_name + instrument

    def __connect(self):
        """
        connect to two redis api. One is to listen to market data,
        another is to write calculate signal in redis.
        These two connections target to different data bases. Specified
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
        sub_channel = self.instrument
        self.sub.subscribe(sub_channel)

    def key_in_redis(self):
        """
        map to key string in Redis
        :return:
        """
        the_key = self.signal_name + '_' + self.instrument + ':' + \
                  (self.digits - len(str(self.counter))) * '0' + \
                  str(self.counter)
        return the_key

    def start(self):
        """
        let signal start running.
        """
        for message in self.sub.listen():
            if message['type'] == 'message':
                str_data = message['data'].decode('utf-8')
                dict_data = json.loads(str_data)
                self.on_message(list(dict_data.values())[0])

    def publish_message(self, dict_message_data):
        """
        set hash table in redis and publish message in its own channel.
        The name of channel is just self.signal_name
        :param dict_message_data: dictionary, the data to be published
        :return:
        """
        # create hash set object in redis.
        published_key = self.key_in_redis()
        self.signal_api.set_dict(published_key, dict_message_data)

        # serialize json dict to string.
        published_message = json.dumps({published_key: dict_message_data})

        # publish the message to support other subscriber.
        self.signal_api.connection.publish(
            channel=self.pub_channel,
            message=published_message
        )

    def on_message(self, message):
        """
        The logic that is being executed every time market data is received.
        :param message:
        :return:
        """
        pass


class NaiveSingleInstrumentSignal(SingleInstrumentSignal):
    """
    A testing implementation of single instrument signal.
    """

    def __init__(self, instrument):
        """
        constructor.
        """
        super(NaiveSingleInstrumentSignal, self).__init__(instrument)

        # reset signal name and pub channel
        self.signal_name = 'naive_test_single'
        self.pub_channel = self.signal_name + instrument

    def on_message(self, message):
        """
        The logic that is being executed every time market data is received.
        Implemented as just printing the data out.
        :param message:
        :return:
        """
        self.publish_message(message)
        self.counter += 1

