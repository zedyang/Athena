import numpy as np
from Athena.settings import AthenaConfig
from Athena.containers import OrderEvent, OrderType
from Athena.strategies.strategy import StrategyTemplate

Tf, Kf = AthenaConfig.HermesTickFields, AthenaConfig.HermesKLineFields

__author__ = 'Atom'


class TransformMarketVol(StrategyTemplate):
    """

    """
    strategy_name_prefix = 'strategy:signal_marketvol'
    param_names = ['ma', 'don_up', 'don_down', 'stop_win', 'trailing']

    def __init__(self, subscribe_list, param_list,
                 instrument, train=False):
        """

        :param subscribe_list:
        :param param_list:
        :param instrument:
        :param train:
        """
        super(TransformMarketVol, self).__init__(subscribe_list)
        self._map_to_channels(param_list, suffix=instrument, full_name=train)

        '''
        '''
        # set parameters

        self.last_bar = {
            Kf.open_price: np.nan,
            Kf.high_price: np.nan,
            Kf.low_price: np.nan,
            Kf.close_price: np.nan,
            'count': -1,
            Kf.contract: None,
            Kf.close_time: None
        }

        # strategy status
        self.tradestate = False

    def signal_score(self, message):
        if message['3'] > 0.05:
            self.tradestate = True

    def __trade_logic(self, message):
        if self.tradestate:
            order = OrderEvent(
                direction='long',
                type=OrderType.cover_short,
                subtype='stop_win',
                quantity=1,
                contract=message[Tf.contract],
                price=message[Tf.last_price],
                update_time=message[Tf.local_time],
                commission=0,
                bar_count=self.last_bar[Kf.count]
            )
            self.publish(order, plot=True)
            self.tradestate = False

    def on_message(self, message):
        """

        :param message:
        :return:
        """
        if message['tag'] == 'market_vol':
            self.signal_score(message)

        elif message['tag'] == 'md':
            self.__trade_logic(message)

        elif message['tag'] == 'kl':
            # update last bar
            for field in Kf.ohlc:
                self.last_bar[field] = float(message[field])
            self.last_bar[Kf.count] = message[Kf.count]
            self.last_bar[Kf.close_time] = message[Kf.close_time]
            self.last_bar[Kf.contract] = message[Kf.contract]