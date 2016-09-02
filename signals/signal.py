import json
from abc import ABCMeta, abstractmethod

from Athena.settings import AthenaConfig
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
    def _map_to_channels(self, param_list):
        """

        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def publish(self, data):
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
    signal_name_prefix = 'signal:template'
    param_names = ['abstract']

    def __init__(self, subscribe_list, duplicate=1):
        """
        constructor.
        """
        self.subscribe_list = subscribe_list
        self.plot_duplicates = duplicate
        self.counter = 0

        # open connection to redis server.
        self.redis_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)
        # create a listener
        self.sub = self.redis_wrapper.connection.pubsub()
        # subscribe channels in the list
        self.sub.subscribe(self.subscribe_list)
        self.sub.subscribe('flags')

        self.tag = 'abstract'

    def _map_to_channels(self, param_list, suffix=None, full_name=False):
        """

        :param param_list:
        :param suffix:
        :param full_name:
        :return:
        """
        self.param_dict = dict(zip(self.param_names, param_list))

        # map to names of publishing channels
        self.pub_channel = self.signal_name_prefix
        if suffix:
            self.pub_channel = self.pub_channel + '.' + suffix
        if full_name:
            self.pub_channel = self.pub_channel + '.' + str(param_list)
        self.signal_name = self.pub_channel

        if self.plot_duplicates == 1:
            self.plot_data_channel = 'plot:' + self.pub_channel
        else:
            self.plot_data_channel = []
            for i in range(self.plot_duplicates):
                self.plot_data_channel.append(
                    'plot_{}:'.format(i) + self.pub_channel
                )

    def start(self):
        """
        let signal start running.
        """
        for message in self.sub.listen():
            if message['type'] == 'message':
                str_data = message['data'].decode('utf-8')
                dict_data = json.loads(str_data)
                d = list(dict_data.values())[0]

                # operations on flags
                if d['tag'] == 'flag':
                    if d['type'] == 'flag_0':
                        return
                else:
                    self.on_message(d)

    def publish(self, data, plot=True):
        """

        :param data:
        :param plot:
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
        if plot:
            if type(self.plot_data_channel) == str:
                # map to new key in Athena db (plotting)
                athena_unique_key_plotting = \
                    append_digits_suffix_for_redis_key(
                        prefix=self.plot_data_channel,
                        counter=self.counter,
                    )

                # publish plotting (dict) data.
                self.redis_wrapper.set_dict(athena_unique_key_plotting, data)

                # publish plotting str message
                plot_message = json.dumps({athena_unique_key_plotting: data})
                self.redis_wrapper.connection.publish(
                    channel=self.plot_data_channel,
                    message=plot_message
                )

            elif type(self.plot_data_channel) == list:
                for i in range(len(self.plot_data_channel)):
                    this_plot_data_channel = self.plot_data_channel[i]
                    # map to new key in Athena db (plotting)
                    athena_unique_key_plotting = \
                        append_digits_suffix_for_redis_key(
                            prefix=this_plot_data_channel,
                            counter=self.counter,
                        )

                    # publish plotting (dict) data.
                    self.redis_wrapper.set_dict(
                        athena_unique_key_plotting, data
                    )

                    # publish plotting str message
                    plot_message = json.dumps(
                        {athena_unique_key_plotting: data}
                    )
                    self.redis_wrapper.connection.publish(
                        channel=this_plot_data_channel,
                        message=plot_message
                    )

        # update the one record for storing last signal
        # note that this 'current' can only be retrieved subjectively
        athena_unique_key_current = self.pub_channel + ':0'
        self.redis_wrapper.set_dict(athena_unique_key_current, data)

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
    signal_name_prefix = 'signal:naive'
    param_names = ['foo', 'bar']

    def __init__(self, subscribe_list, param_list):
        """
        constructor.
        """
        super(NaiveSignal, self).__init__(subscribe_list)
        self._map_to_channels(param_list)

        self.tag = 'naive'

    def on_message(self, message):
        """
        The logic that is being executed every time market data is received.
        Implemented as just printing the data out.
        :param message:
        :return:
        """
        self.publish({'foo': self.counter})

