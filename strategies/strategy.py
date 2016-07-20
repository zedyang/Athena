import json
from abc import ABCMeta, abstractmethod

from Athena.settings import AthenaConfig
from Athena.containers import OrderEvent
from Athena.utils import append_digits_suffix_for_redis_key
from Athena.db_wrappers.redis_wrapper import RedisWrapper

__author__ = 'zed'


class Strategy(object):
    """
    The strategies object is the one that is responsible for synthesizing
    one or more signals into buy/sell order signals.

    Strategy object opens connection to redis and listens to multiple
    md, signal channels. On receiving the message, the strategies execute its
    inner logic to push an order message into redis.

    concrete strategies class should implement __subscribe,
    publish and on_message method, which is pretty much the same as
    the signal class.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def __subscribe(self):
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
    def __init__(self, subscribe_list):
        """
        constructor
        :param subscribe_list: list of string, the list of subscribed signals.
        The string should be exactly same as the pub_channel of that signal,
        which is also the data directory of that signal in redis.
        """
        self.strategy_name = 'strategy:template'
        self.subscribe_list = subscribe_list

        self.counter = 0
        self.__subscribe()

        # set the name of pub channels
        self.pub_channel = self.strategy_name
        self.plot_data_channel = 'plot:' + self.pub_channel

    def __subscribe(self):
        """
        - open two connections to redis, one is to listen to md and signals
        as input to strategy, another is to write buy/sell signals generated
        by redis itself.
        - subscribe to some signals/md by listening to the channel in redis.
        :return:
        """
        # make connections
        self.sub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)
        self.pub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)
        # create a sub.
        self.sub = self.sub_wrapper.connection.pubsub()
        # subscribe channels in the subscribe_list
        self.sub.subscribe(self.subscribe_list)

    def start(self):
        """
        begin the loop of strategies.
        :return:
        """
        for message in self.sub.listen():
            if message['type'] == 'message':
                str_data = message['data'].decode('utf-8')
                dict_data = json.loads(str_data)
                self.on_message(list(dict_data.values())[0])

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
        order_dict['type'] = 'order'
        order_dict['tag'] = self.strategy_name

        self.pub_wrapper.set_dict(published_key, order_dict)

        # serialize json dict to string.
        published_message = json.dumps({published_key: order_dict})

        # publish the message to support other subscriber.
        self.pub_wrapper.connection.publish(
            channel=self.pub_channel,
            message=published_message
        )

        if plot:
            # create hash set object in redis.
            published_key = append_digits_suffix_for_redis_key(
                prefix=self.plot_data_channel,
                counter=self.counter
            )
            plot_data = {
                'direction': order_dict['direction'],
                'price': order_dict['price'],
                'update_time': order_dict['update_time'],
                'bar_count': order_dict['bar_count']
            }
            self.pub_wrapper.set_dict(published_key, plot_data)

            # serialize json dict to string.
            published_message = json.dumps({published_key: plot_data})

            # publish the message to support other subscriber.
            self.pub_wrapper.connection.publish(
                channel=self.plot_data_channel,
                message=published_message
            )

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
    def __init__(self, subscribe_list):
        """
        constructor.
        """
        super(NaiveTestStrategy, self).__init__(subscribe_list)

        # reset signal name and pub channel
        self.signal_name = 'strategy:naive_test_strategy'
        self.pub_channel = self.signal_name

    def on_message(self, message):
        """
        The logic that is being executed every time signal is received.
        Implemented as just publishing everything.
        :param message:
        :return:
        """
        self.publish(message)
        self.counter += 1