import json
from abc import ABCMeta, abstractmethod

from Athena.settings import AthenaConfig
from Athena.db_wrappers.redis_wrapper import RedisWrapper
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
    def __subscribe(self, channels):
        """

        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def _publish(self, data):
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


# -----------------------------------------------------------------------
class SignalTemplate(Signal):
    """
    Template class that implements general methods that is used in all
    derived signals commonly. The customized signal class should inherit
    SignalTemplate.

    The signal object is only running on athena db, hence it suffices to
    open one connection.
    """

    def __init__(self, sub_channels):
        """
        constructor.
        """
        # open connection to redis server.
        self.redis_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)

        # create a listener
        self.sub = self.redis_wrapper.connection.pubsub()
        self.sub_channels = sub_channels
        self.sub_names = [c.replace(':', '.') for c in sub_channels]

        # set signal name and publishing channels,
        # these MUST be reset in derived signals.
        self.tag = 'abstract'
        self.signal_name = 'signal:abstract'
        self.pub_channel = self.signal_name
        self.pub_channel_plot = 'plot:' + self.pub_channel

        # set counter to make redis keys
        self.counter = 0

        self.__subscribe(sub_channels)

    def __subscribe(self, channels):
        """

        :return:
        """
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

    def _publish(self, data):
        """
        publish data into redis db.
        :param data:
        :return:
        """
        # map to new key in Athena db.
        athena_unique_key = append_digits_suffix_for_redis_key(
            prefix=self.pub_channel,
            counter=self.counter,
        )

        # append tag field to dict data
        if self.tag:
            data['tag'] = self.tag

        # publish dict data
        self.redis_wrapper.set_dict(athena_unique_key, data)

        # publish str message
        # first serialize json dict to string.
        message = json.dumps({athena_unique_key: data})
        self.redis_wrapper.connection.publish(
            channel=self.pub_channel,
            message=message
        )

        # publish plotting data

        # map to new key in Athena db (plotting)
        athena_unique_key_plotting = append_digits_suffix_for_redis_key(
            prefix=self.pub_channel_plot,
            counter=self.counter,
        )

        # publish plotting (dict) data.
        self.redis_wrapper.set_dict(athena_unique_key_plotting, data)

        # publish plotting str message
        plot_message = json.dumps({athena_unique_key_plotting: data})
        self.redis_wrapper.connection.publish(
            channel=self.pub_channel_plot,
            message=plot_message
        )

        # increment to counter
        self.counter += 1

    def on_message(self, message):
        """
        The logic that is being executed every time market data is received.
        :param message:
        :return:
        """
        pass


class NaiveSignal(SignalTemplate):
    """
    A testing implementation of signal.
    """

    def __init__(self, sub_channels):
        """
        constructor.
        """
        super(NaiveSignal, self).__init__(sub_channels)

        # reset signal name and pub channel
        self.tag = 'naive'
        self.signal_name = 'signal:naive'
        self.pub_channel = self.signal_name
        self.pub_channel_plot = 'plot:' + self.pub_channel

    def on_message(self, message):
        """
        The logic that is being executed every time market data is received.
        Implemented as just printing the data out.
        :param message:
        :return:
        """
        self._publish(message)

