import numpy as np

from Athena.settings import AthenaConfig, AthenaProperNames
from Athena.containers import OrderEvent
from Athena.strategies.strategy import StrategyTemplate

__author__ = 'zed'


class MACrossStrategy(StrategyTemplate):
    """

    """
    tag_ma = 'ma'
    tag_bar = 'bar'

    def __init__(self, subscribe_list):
        """

        """
        super(MACrossStrategy, self).__init__(subscribe_list)

        self.signals = {
            'long': None,
            'short': None
        }
        self.has_position = False

        # reset strategies pub channel
        self.strategy_name = 'strategy:ma_cross'
        self.pub_channel = self.strategy_name
        self.plot_data_channel = 'plot:' + self.pub_channel

    def on_message(self, message):
        """

        :param message:
        :return:
        """
        if 'tag' in message:
            self.signals['short'] = float(message['36'])
            self.signals['long'] = float(message['48'])
        else:   # on bars
            if self.signals['long']:  # when ma long has data

                if not self.has_position and \
                        self.signals['short'] > (
                        self.signals['long']):
                    # create and publish order event.
                    order = OrderEvent(
                        direction=AthenaProperNames.long,
                        quantity=10,
                        contract=message[AthenaConfig.sql_instrument_field],
                        price=message['close'],
                        update_time=message['open_time'],
                        commission=0,
                        bar_count=message['count']
                    )
                    self.publish(order, plot=True)
                    self.has_position = True
                    self.counter += 1

                if self.has_position and \
                        self.signals['short'] < (
                        self.signals['long']):
                    order = OrderEvent(
                        direction=AthenaProperNames.short ,
                        quantity=10,
                        contract=message[AthenaConfig.sql_instrument_field],
                        price=message['close'],
                        update_time=message['open_time'],
                        commission=0,
                        bar_count = message['count']
                    )
                    self.publish(order, plot=True)
                    self.has_position = False
                    self.counter += 1


