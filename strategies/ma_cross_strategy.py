import numpy as np

from Athena.settings import AthenaConfig, AthenaProperNames
from Athena.containers import OrderEvent
from Athena.strategies.strategy import StrategyTemplate
Kf = AthenaConfig.KLineFields

__author__ = 'zed'


class MACrossStrategy(StrategyTemplate):
    """

    """
    tag_ma = 'ma'
    tag_bar = 'kl'

    def __init__(self, subscribe_list, ma_short, ma_long):
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
        self.table_data_channel = 'table:' + self.pub_channel

    def on_message(self, message):
        """

        :param message:
        :return:
        """
        if message['tag'] == 'ma':
            self.signals['short'] = float(message['36'])
            self.signals['long'] = float(message['48'])

        elif message['tag'] == 'kl':

            if self.signals['long']:  # when ma long has data

                if not self.has_position and \
                        self.signals['short'] > (
                        self.signals['long']):
                    # create and publish order event.
                    order = OrderEvent(
                        direction=AthenaProperNames.long,
                        subtype='open_long',
                        quantity=10,
                        contract=message[Kf.contract],
                        price=message[Kf.close_price],
                        update_time=message[Kf.end_time],
                        commission=0,
                        bar_count=message['count']
                    )

                    self.publish(order, plot=True)
                    self.has_position = True

                if self.has_position and \
                        self.signals['short'] < (
                        self.signals['long']):
                    order = OrderEvent(
                        direction=AthenaProperNames.short,
                        subtype='close',
                        quantity=10,
                        contract=message[Kf.contract],
                        price=message[Kf.close_price],
                        update_time=message[Kf.end_time],
                        commission=0,
                        bar_count = message['count']
                    )

                    self.publish(order, plot=True)
                    self.has_position = False



