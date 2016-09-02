import numpy as np

from Athena.settings import AthenaConfig
from Athena.containers import OrderEvent, OrderType
from Athena.strategies.strategy import StrategyTemplate

Tf, Kf = AthenaConfig.HermesTickFields, AthenaConfig.HermesKLineFields

__author__ = 'zed'


class CTAStrategy1(StrategyTemplate):
    """

    """
    strategy_name_prefix = 'strategy:cta_1'
    param_names = ['ma_short', 'ma_long', 'don', 'stop_win', 'break_control']

    opening_signal_fields = [
        'up_break', 'down_break',
        'up_control', 'down_control',
        'ma_short', 'ma_long'
    ]
    closing_signal_fields = [
        'tracking_high', 'tracking_low',
    ]

    def __init__(self, subscribe_list, param_list,
                 instrument, train=False):
        """

        :param subscribe_list:
        :param param_list:
        :param instrument:
        :param train:
        """
        super(CTAStrategy1, self).__init__(subscribe_list)
        self._map_to_channels(param_list, suffix=instrument,
                              full_name=train)

        # set parameters
        self.ma_window_widths = {
            'ma_short': param_list[0],
            'ma_long': param_list[1]
        }
        self.stop_win_threshold = self.param_dict['stop_win']
        self.break_control_threshold = self.param_dict['break_control']

        # containers
        self.opening_signal_list = {
            'up_break': np.nan,
            'down_break': np.nan,
            'up_control': np.nan,
            'down_control': np.nan,
            'ma_short': np.nan,
            'ma_long': np.nan
        }

        self.closing_signal_list = {
            'tracking_high': np.nan,
            'tracking_low': np.nan
        }

        self.last_bar = {
            Kf.open_price: np.nan,
            Kf.high_price: np.nan,
            Kf.low_price: np.nan,
            Kf.close_price: np.nan,
            'count': 0,
            Kf.contract: None,
            Kf.close_time: None
        }

        # strategy status
        self.has_position = False
        self.watch_open = False
        self.start_stoploss_logic = False
        self.start_stopwin_logic = False

    def __watch_open_logic(self, message):
        """

        :return:
        """
        if not self.has_position:

            # unzip signal list
            (up_break, down_break, up_control,
             down_control, ma_short, ma_long) = (
                self.opening_signal_list[field]
                for field in CTAStrategy1.opening_signal_fields
            )
            # watch buy status
            if (
                        ma_short > ma_long
            ) and (
                        up_break > 0
            ) and (
                        up_control < self.break_control_threshold
            ):
                # make change status event
                order = OrderEvent(
                    direction=None,
                    type=None,
                    subtype='watch_buy',
                    quantity=0,
                    contract=None,
                    price=np.nan,
                    update_time=self.last_bar[Kf.close_time],
                    commission=0,
                    bar_count=self.last_bar['count']
                )
                self.publish(order, plot=True)

                self.watch_open = 'long'
                return

            # watch sell status
            if (
                        ma_short < ma_long
            ) and (
                        down_break > 0
            ) and (
                        down_control < self.break_control_threshold
            ):
                # make change status event
                order = OrderEvent(
                    direction=None,
                    type=None,
                    subtype='watch_sell',
                    quantity=0,
                    contract=self.last_bar[Kf.contract],
                    price=np.nan,
                    update_time=self.last_bar[Kf.close_time],
                    commission=0,
                    bar_count=self.last_bar['count']
                )
                self.publish(order, plot=True)

                self.watch_open = 'short'
                return

            # otherwise, exit watch open status

            if self.watch_open:
                # make change status event
                order = OrderEvent(
                    direction=None,
                    type=None,
                    subtype='watch_exit',
                    quantity=0,
                    contract=self.last_bar[Kf.contract],
                    price=np.nan,
                    update_time=self.last_bar[Kf.close_time],
                    commission=0,
                    bar_count=self.last_bar['count']
                )
                self.publish(order, plot=True)

            self.watch_open = False

    def __open_logic(self, message):
        """

        :return:
        """
        if not self.has_position:
            last_price = float(message[Tf.last_price])

            # open long
            if self.watch_open == 'long':
                if last_price >= self.last_bar[Kf.high_price]:
                    # make long order event
                    order = OrderEvent(
                        direction='long',
                        type=OrderType.open_long,
                        subtype='open_long',
                        quantity=1,
                        contract=message[Tf.contract],
                        price=last_price,
                        update_time=message[Tf.local_time],
                        commission=0.05,
                        bar_count=self.last_bar['count']
                    )
                    self.publish(order, plot=True)
                    self.has_position = 'long'
                    self.watch_open = False

            # open short
            elif self.watch_open == 'short':
                if last_price <= self.last_bar[Kf.low_price]:
                    # make short order event
                    order = OrderEvent(
                        direction='short',
                        type=OrderType.open_short,
                        subtype='open_short',
                        quantity=1,
                        contract=message[Tf.contract],
                        price=last_price,
                        update_time=message[Tf.local_time],
                        commission=0.05,
                        bar_count=self.last_bar['count']
                    )
                    self.publish(order, plot=True)
                    self.has_position = 'short'
                    self.watch_open = False

            else:
                return

    def __stoploss_logic(self, message):
        """

        :param message:
        :return:
        """
        if self.closing_signal_list['tracking_high'] is np.nan:
            # does not have trailing stop signal yet
            return

        # otherwise
        last_price = float(message[Tf.last_price])

        # close long
        if self.has_position == 'long':
            if last_price < self.closing_signal_list['tracking_low'] - 1:
                # make short order event (close long)
                order = OrderEvent(
                    direction='short',
                    type=OrderType.cover_long,
                    subtype='stop_loss',
                    quantity=1,
                    contract=message[Tf.contract],
                    price=last_price,
                    update_time=message[Tf.local_time],
                    commission=0,
                    bar_count=self.last_bar['count']
                )
                self.publish(order, plot=True)

                self.has_position = False
                self.start_stoploss_logic = False
                self.start_stopwin_logic = False

        elif self.has_position == 'short':
            if last_price > self.closing_signal_list['tracking_high'] + 1:
                # make long order event (close short)
                order = OrderEvent(
                    direction='long',
                    type=OrderType.cover_short,
                    subtype='stop_loss',
                    quantity=1,
                    contract=message[Tf.contract],
                    price=last_price,
                    update_time=message[Tf.local_time],
                    commission=0,
                    bar_count=self.last_bar[Kf.count]
                )
                self.publish(order, plot=True)

                self.has_position = False
                self.start_stoploss_logic = False
                self.start_stopwin_logic = False

    def __stopwin_logic(self, message):
        """

        :param message:
        :return:
        """
        if self.closing_signal_list['tracking_high'] is np.nan:
            # does not have trailing stop signal yet
            return

        # otherwise
        close_price = message[Kf.close_price]
        # close long
        if self.has_position == 'long':
            low_price = message[Kf.low_price]
            if low_price < self.closing_signal_list['tracking_high'] * (
                        1 - self.stop_win_threshold):
                # make short order event (close long)
                order = OrderEvent(
                    direction='short',
                    type=OrderType.cover_long,
                    subtype='stop_win',
                    quantity=1,
                    contract=message[Kf.contract],
                    price=close_price,
                    update_time=message[Kf.close_time],
                    commission=0,
                    bar_count=self.last_bar[Kf.count]
                )
                self.publish(order, plot=True)

                self.has_position = False
                self.start_stoploss_logic = False
                self.start_stopwin_logic = False

        elif self.has_position == 'short':
            high_price = message[Kf.high_price]
            if high_price > self.closing_signal_list['tracking_low'] * (
                        1 + self.stop_win_threshold):
                # make long order event (close short)
                order = OrderEvent(
                    direction='long',
                    type=OrderType.cover_short,
                    subtype='stop_win',
                    quantity=1,
                    contract=message[Kf.contract],
                    price=close_price,
                    update_time=message[Kf.close_time],
                    commission=0,
                    bar_count=self.last_bar[Kf.count]
                )
                self.publish(order, plot=True)

                self.has_position = False
                self.start_stoploss_logic = False
                self.start_stopwin_logic = False

    def on_message(self, message):
        """

        :param message:
        :return:
        """
        if message['tag'] == 'don':
            # on donchian signal event
            for field in CTAStrategy1.opening_signal_fields[:4]:
                self.opening_signal_list[field] = float(
                    message[field + '_{}'.format(self.param_dict['don'])])

            # if signal list is complete
            if np.nan not in self.opening_signal_list.values():
                # execute watch open logic
                self.__watch_open_logic(message)

        elif message['tag'] == 'ma':
            # on ma signal event
            for field in CTAStrategy1.opening_signal_fields[4:]:
                self.opening_signal_list[field] = float(
                    message[str(self.ma_window_widths[field])])

            # if signal list is complete
            if np.nan not in self.opening_signal_list.values():
                # execute watch open logic
                self.__watch_open_logic(message)

        elif message['tag'] == 'md':
            # on md (tick) event
            # opening position is executed on tick.
            self.__open_logic(message)

            # stop loss on ticks
            if self.start_stoploss_logic:
                self.__stoploss_logic(message)

        elif message['tag'] == 'kl':
            # on bars
            # stop win on bars
            if self.start_stopwin_logic:
                self.__stopwin_logic(message)

            if self.has_position:
                # if a position is opened on tick within this bar
                self.start_stoploss_logic = True
                self.start_stopwin_logic = True

            # update last bar
            for field in Kf.ohlc:
                self.last_bar[field] = float(message[field])
            self.last_bar['count'] += 1
            self.last_bar[Kf.close_time] = message[Kf.close_time]
            self.last_bar[Kf.contract] = message[Kf.contract]

        elif message['tag'] == 'stop':
            # on trailing stop signals.
            for field in CTAStrategy1.closing_signal_fields:
                self.closing_signal_list[field] = float(message[field])
