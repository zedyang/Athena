import json

from Athena.settings import AthenaConfig
from Athena.apis.database_api import RedisAPI
from Athena.signals.signal import Signal

__author__ = 'zed'


class DerivedSignal(Signal):
    """
    Implementation of Signal object that is derived from another signal.
    The pub channel is signal_name + '_' + sub_channel
    and the sub channel is an input parameter on initialization.
    """

    def __init__(self, sub_channel):
        """
        constructor
        :param sub_channel: the channel which derived signal subscribes to
        """

        self.signal_name = 'abstract_derived'
        self.sub_channel = sub_channel
        self.pub_channel = self.signal_name + sub_channel

        self.digits = AthenaConfig.ATHENA_REDIS_MD_MAX_DIGITS
        self.counter = 0

        self.__connect()
        self.__subscribe_instruments()

    def __connect(self):
        """
        connect to two redis api. One is to listen to underlying signal,
        another is to write derived signal.
        These two connections target to db3
        :return:
        """
        # the connection that subscribes market data.
        self.underlying_api = RedisAPI(
            db=AthenaConfig.ATHENA_REDIS_SIGNAL_DB_INDEX)

        # the connection that publishes signal
        self.own_api = RedisAPI(
            db=AthenaConfig.ATHENA_REDIS_SIGNAL_DB_INDEX)

    def __subscribe_instruments(self):
        """
        subscribe the underlying signal.
        :return:
        """
        # create a sub.
        self.sub = self.underlying_api.connection.pubsub()
        # channel directory (set in MarketDataHandler
        channel = self.sub_channel
        self.sub.subscribe(channel)

    def key_in_redis(self):
        """
        map to key string in Redis
        :return:
        """
        the_key = self.signal_name + '_' + self.sub_channel + ':' + \
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
        self.own_api.set_dict(published_key, dict_message_data)

        # serialize json dict to string.
        published_message = json.dumps({published_key: dict_message_data})

        # publish the message to support other subscriber.
        self.own_api.connection.publish(
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


class NaiveDerivedSignal(DerivedSignal):
    """
    A testing implementation of single instrument signal.
    output 0 if the md is BID(SLD), 1 if ask (BOT).
    """

    def __init__(self, sub_channel):
        """
        constructor.
        """
        super(NaiveDerivedSignal, self).__init__(sub_channel)
        self.signal_name = 'naive_test_derived'
        self.pub_channel = self.signal_name + '_' + sub_channel

    def on_message(self, message):
        """
        The logic that is being executed every time market data is received.
        Implemented as just printing the data out.
        :param message:
        :return:
        """
        self.publish_message(message)
        self.counter += 1
