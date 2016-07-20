import json
from abc import ABCMeta, abstractmethod

from Athena.settings import AthenaConfig, AthenaProperNames
from Athena.data_handler.redis_wrapper import RedisWrapper
from Athena.utils import append_digits_suffix_for_redis_key

__author__ = 'zed'


# -----------------------------------------------------------------------
class Signal(object):
    """
    The abstract signal class, provides interfaces to all derived signal
    types. A signal will open one or more connections to the redis database.

    A signal model essentially subscribe and publish from/to some channels.
    For example, a single instrument signal will only listens to one channel:
    market data of the instrument it is tracking.

    on_message() is a callback function that is executed every time receiving
    a message on the listening channels. It will examine the type of
    messages and branches to execute corresponding internal logic.

    __publish() method is to write this hash set and publish message in
    its own channel that other entities might listens to.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def __subscribe(self):
        """

        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def __publish(self, data):
        """

        :param data:
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
    i.e. one market data instrument.
    The pub channel is signal_name + '_' + 'instrument'
    and the sub channel is just instrument.

    The class inherits SingleInstrument signal must reset signal_name
    and pub_channel.
    """

    def __init__(self, instrument, tag=None):
        """
        constructor.
        :param instrument: the market instrument that the signal listens to.
        :param tag: usr defined tag as a dependent field in message.

        """
        self.signal_name = 'signal:abstract_single'
        self.instrument = instrument
        self.tag = tag

        self.counter = 0
        self.__subscribe()

        # set the name of pub channels
        self.pub_channel = self.signal_name + '_' + instrument
        self.pub_channel_plot = 'plot' + self.pub_channel

    def __subscribe(self):
        """
        - open two connections to redis. One is to listen to market data,
        another is to write calculated signals.
        - subscribe some instrument by listening to the channel in Redis.
        :return:
        """
        # open two connections
        self.md_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)
        self.signal_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)

        # create a sub.
        self.sub = self.md_wrapper.connection.pubsub()

        # channel directory (set in MarketDataHandler
        sub_channel = 'md:' + self.instrument
        self.sub.subscribe(sub_channel)

    def start(self):
        """
        let signal start running.
        """
        for message in self.sub.listen():
            if message['type'] == 'message':
                str_data = message['data'].decode('utf-8')
                dict_data = json.loads(str_data)
                self.on_message(list(dict_data.values())[0])

    def publish(self, dict_message_data, plot=True):
        """
        set hash table in redis and publish message in its own channel.
        The name of channel is just self.signal_name
        :param dict_message_data: dictionary, the data to be published
        :param plot: bool, whether to publish plot data as well.
        :return:
        """
        # create hash set object in redis.
        dict_message_data['type'] = AthenaProperNames.signal_message_type
        if self.tag:
            dict_message_data['tag'] = self.tag

        published_key = append_digits_suffix_for_redis_key(
            prefix=self.signal_name + '_' + self.instrument,
            counter=self.counter,
        )
        self.signal_wrapper.set_dict(published_key, dict_message_data)

        # serialize json dict to string.
        published_message = json.dumps({published_key: dict_message_data})

        # publish the message to support other subscriber.
        self.signal_wrapper.connection.publish(
            channel=self.pub_channel,
            message=published_message
        )

        # publish data for plotting if needed
        if plot:
            published_key_plot = append_digits_suffix_for_redis_key(
                prefix=self.pub_channel_plot,
                counter=self.counter,
            )
            self.signal_wrapper.set_dict(published_key_plot, dict_message_data)
            self.signal_wrapper.connection.publish(
                channel=self.pub_channel_plot,
                message=published_message
            )

    def on_message(self, message):
        """
        The logic that is being executed every time market data is received.
        :param message:
        :return:
        """
        pass


class DerivedSignal(Signal):
    """
    Implementation of Signal object that is derived from another signal.
    The pub channel is signal_name + '_' + sub_channel
    and the sub channel is an input parameter on initialization.
    """

    def __init__(self, sub_channel, tag=None):
        """
        constructor
        :param sub_channel: the channel which derived signal subscribes to
        """

        self.signal_name = 'signal:abstract_derived'
        self.tag = tag
        self.sub_channel = sub_channel
        self.sub_signal = sub_channel.split(':')[-1]
        self.pub_channel = self.signal_name + '_' + self.sub_signal
        self.pub_channel_plot = 'plot:' + self.pub_channel

        self.counter = 0

        self.__subscribe()

    def __subscribe(self):
        """
        subscribe the underlying signal.
        :return:
        """
        # open two connections
        self.sub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)
        self.pub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)

        # create a sub.
        self.sub = self.sub_wrapper.connection.pubsub()

        # channel directory (set in MarketDataHandler
        channel = self.sub_channel
        self.sub.subscribe(channel)

    def start(self):
        """
        let signal start running.
        """
        for message in self.sub.listen():
            if message['type'] == 'message':
                str_data = message['data'].decode('utf-8')
                dict_data = json.loads(str_data)
                self.on_message(list(dict_data.values())[0])

    def publish(self, dict_message_data, plot=True):
        """
        set hash table in redis and publish message in its own channel.
        The name of channel is just self.signal_name
        :param dict_message_data: dictionary, the data to be published
        :param plot: bool, whether to publish plot data as well.
        :return:
        """
        # create hash set object in redis.
        dict_message_data['type'] = AthenaProperNames.signal_message_type
        if self.tag:
            dict_message_data['tag'] = self.tag

        published_key = append_digits_suffix_for_redis_key(
            prefix=self.pub_channel,
            counter=self.counter,
        )
        self.pub_wrapper.set_dict(published_key, dict_message_data)

        # serialize json dict to string.
        published_message = json.dumps({published_key: dict_message_data})

        # publish the message to support other subscriber.
        self.pub_wrapper.connection.publish(
            channel=self.pub_channel,
            message=published_message
        )

        # publish data for plotting if needed
        if plot:
            published_key_plot = append_digits_suffix_for_redis_key(
                prefix=self.pub_channel_plot,
                counter=self.counter,
            )
            self.pub_wrapper.set_dict(published_key_plot, dict_message_data)
            self.pub_wrapper.connection.publish(
                channel=self.pub_channel_plot,
                message=published_message
            )

    def on_message(self, message):
        """
        The logic that is being executed every time market data is received.
        :param message:
        :return:
        """
        pass


class MacroscopicSignal(Signal):
    """
    Implementation of Signal object that is derived from a combination of
    md, signals, positions or strategy status. This is the most general case.
    The pub channel is signal_name + '_' + sub_channel
    and the sub channel is an input parameter on initialization.
    """

    def __init__(self, sub_channel_list, tag=None):
        """
        constructor
        :param sub_channel: the channel which derived signal subscribes to
        """

        self.signal_name = 'signal:abstract_macro'
        self.tag = tag
        self.sub_channel_list = sub_channel_list

        self.pub_channel = self.signal_name
        self.pub_channel_plot = 'plot:' + self.pub_channel

        self.counter = 0

        self.__subscribe()

    def __subscribe(self):
        """
        subscribe the underlying signal.
        :return:
        """
        # open two connections
        self.sub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)
        self.pub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)

        # create a sub.
        self.sub = self.sub_wrapper.connection.pubsub()

        # channel directory (set in MarketDataHandler
        channels = self.sub_channel_list
        self.sub.subscribe(channels)

    def start(self):
        """
        let signal start running.
        """
        for message in self.sub.listen():
            if message['type'] == 'message':
                str_data = message['data'].decode('utf-8')
                dict_data = json.loads(str_data)
                self.on_message(list(dict_data.values())[0])

    def publish(self, dict_message_data, plot=True):
        """
        set hash table in redis and publish message in its own channel.
        The name of channel is just self.signal_name
        :param dict_message_data: dictionary, the data to be published
        :param plot: bool, whether to publish plot data as well.
        :return:
        """
        # create hash set object in redis.
        dict_message_data['type'] = AthenaProperNames.signal_message_type
        if self.tag:
            dict_message_data['tag'] = self.tag

        published_key = append_digits_suffix_for_redis_key(
            prefix=self.pub_channel,
            counter=self.counter,
        )
        self.pub_wrapper.set_dict(published_key, dict_message_data)

        # serialize json dict to string.
        published_message = json.dumps({published_key: dict_message_data})

        # publish the message to support other subscriber.
        self.pub_wrapper.connection.publish(
            channel=self.pub_channel,
            message=published_message
        )

        # publish data for plotting if needed
        if plot:
            published_key_plot = append_digits_suffix_for_redis_key(
                prefix=self.pub_channel_plot,
                counter=self.counter,
            )
            self.pub_wrapper.set_dict(published_key_plot, dict_message_data)
            self.pub_wrapper.connection.publish(
                channel=self.pub_channel_plot,
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

    def __init__(self, instrument, tag):
        """
        constructor.
        """
        super(NaiveSingleInstrumentSignal, self).__init__(instrument, tag)

        # reset signal name and pub channel
        self.signal_name = 'signal:naive_test_single'
        self.pub_channel = self.signal_name + '_' + instrument

    def on_message(self, message):
        """
        The logic that is being executed every time market data is received.
        Implemented as just printing the data out.
        :param message:
        :return:
        """
        self.publish(message)
        self.counter += 1
