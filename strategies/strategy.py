import json
from abc import ABCMeta, abstractmethod, abstractproperty

from Athena.settings import AthenaConfig
from Athena.containers import OrderEvent
from Athena.utils import append_digits_suffix_for_redis_key
from Athena.data_handler.redis_wrapper import RedisWrapper

__author__ = 'zed'


class Strategy(object):
    """
    The strategies object is the one that is responsible for synthesizing
    one or more signals into buy/sell order signals.

    Strategy object opens connection to redis and listens to multiple
    md, signal channels. On receiving the message, the strategies execute its
    inner logic to push an order message into redis.

    concrete strategies class should implement _map_to_channels,
    publish and on_message method, which is pretty much the same as
    the signal class.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def _map_to_channels(self, param_list):
        """

        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def publish(self, dict_message_data):
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


class StrategyTemplate(Strategy):
    """
    A template strategy class from which other implementations of strategies
    inherits.
    """
    strategy_name_prefix = 'strategy:template'
    param_names = ['abstract']

    def __init__(self, subscribe_list, duplicate=1):
        """
        constructor
        :param subscribe_list: list of string, the list of subscribed signals.
        The string should be exactly same as the pub_channel of that signal,
        which is also the data directory of that signal in redis.
        """
        self.subscribe_list = subscribe_list
        self.counter = 0

        # open connection
        self.redis_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)
        # create a sub.
        self.sub = self.redis_wrapper.connection.pubsub()
        # subscribe channels in the subscribe_list
        self.sub.subscribe(self.subscribe_list)
        self.sub.subscribe('flags')

    def _map_to_channels(self, param_list, suffix=None, full_name=False):
        """

        :param param_list:
        :param suffix:
        :return:
        """
        # set parameter dictionary
        self.param_dict = dict(zip(self.param_names, param_list))

        # map to names of publishing channels
        self.pub_channel  = self.strategy_name_prefix
        if suffix:
            self.pub_channel = self.pub_channel + '.' + suffix
        if full_name:
            self.pub_channel = self.pub_channel + '.' + str(param_list)
        self.strategy_name = self.pub_channel

        self.plot_data_channel = 'plot:' + self.pub_channel
        self.table_data_channel = 'table:' + self.pub_channel

    def start(self):
        """
        begin the loop of strategies.
        :return:
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

    def publish(self, order_event, plot=False):
        """
        set hash table in redis and publish message in its own channel.
        The name of channel is just self.signal_name
        :param order_event: namedtuple 'OrderEvent',
        the data to be published
        :param plot: bool, whether to publish plotting data.
        :return:
        """
        # create hash set object in redis.
        published_key = append_digits_suffix_for_redis_key(
            prefix=self.strategy_name,
            counter=self.counter
        )
        order_dict = dict(order_event._asdict())
        order_dict['tag'] = self.strategy_name

        self.redis_wrapper.set_dict(published_key, order_dict)

        # serialize json dict to string.
        published_message = json.dumps({published_key: order_dict})

        # publish the message to support other subscriber.
        self.redis_wrapper.connection.publish(
            channel=self.pub_channel,
            message=published_message
        )

        if plot:
            # create hash set object in redis.
            published_key = append_digits_suffix_for_redis_key(
                prefix=self.plot_data_channel,
                counter=self.counter
            )

            self.redis_wrapper.set_dict(published_key, order_dict)

            # publish the message to support other subscriber.
            self.redis_wrapper.connection.publish(
                channel=self.plot_data_channel,
                message=published_message
            )

            # table updating
            published_key = append_digits_suffix_for_redis_key(
                prefix=self.table_data_channel,
                counter=self.counter
            )

            self.redis_wrapper.set_dict(published_key, order_dict)

            # publish the message to support other subscriber.
            self.redis_wrapper.connection.publish(
                channel=self.table_data_channel,
                message=published_message
            )

        # increment to counter
        self.counter += 1

    def on_message(self, message):
        """
        The logic that is being executed every time signal is received.
        :param message:
        :return:
        """
        pass


class NaiveTestStrategy(StrategyTemplate):
    """

    """
    strategy_name_prefix = 'strategy:naive'
    param_names = ['foo', 'bar']

    def __init__(self, subscribe_list, param_list):
        """
        constructor.
        """
        super(NaiveTestStrategy, self).__init__(subscribe_list)
        self._map_to_channels(param_list)

    def on_message(self, message):
        """
        The logic that is being executed every time signal is received.
        Implemented as just publishing everything.
        :param message:
        :return:
        """
        to_publish = OrderEvent(
            direction=None,
            subtype=None,
            quantity=None,
            price=None,
            contract=None,
            commission=None,
            update_time=None,
            bar_count=None
        )
        self.publish(to_publish)
        self.counter += 1
