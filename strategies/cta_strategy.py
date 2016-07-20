import numpy as np

from Athena.settings import AthenaConfig, AthenaProperNames
from Athena.containers import OrderEvent
from Athena.strategies.strategy import StrategyTemplate

Tf, Kf = AthenaConfig.TickFields, AthenaConfig.KLineFields

__author__ = 'zed'


class CTAStrategy1(StrategyTemplate):
    """

    """
    tag_ma = 'ma'
    tag_don = 'don'
    opening_signal_fields = [
        'up_break', 'down_break',
        'up_control', 'down_control',
        'ma_short', 'ma_long'
    ]
    closing_signal_fields = [
        'tracking_high', 'tracking_low',
    ]
    bar_fields = [
        'open', 'high', 'low', 'close'
    ]
    ma_window_widths = {
        'ma_short': '36',
        'ma_long': '48'
    }

    def __init__(self, subscribe_list):
        """

        :param subscribe_list:
        :return:
        """
        super(CTAStrategy1, self).__init__(subscribe_list)

        # reset strategies pub channel
        self.strategy_name = 'strategy:cta_1'
        self.pub_channel = self.strategy_name
        self.plot_data_channel = 'plot:' + self.pub_channel

        self.opening_signal_list = {
            'up_break' : np.nan,
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
            'open': np.nan,
            'high': np.nan,
            'low': np.nan,
            'close': np.nan,
            'count': 0
        }

        # strategy status
        self.has_position = False
        self.watch_open = False
        self.start_stoploss_logic = False
        self.start_stopwin_logic = False

        # preset parameters
        self.break_control_threshold = 0.4
        self.stop_win_threshold = 0.0016

    def __watch_open_logic(self):
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
                self.watch_open = 'short'
                return

            # otherwise, exit watch open status
            self.watch_open = False

    def __open_logic(self, message):
        """

        :return:
        """
        if not self.has_position:
            last_price = float(message['last_price'])

            # open long
            if self.watch_open == 'long':
                if last_price >= self.last_bar['high']:

                    # make long order event
                    order = OrderEvent(
                        direction=AthenaProperNames.long,
                        subtype='open',
                        quantity=10,
                        contract=message[Tf.contract],
                        price=last_price,
                        update_time=message[Tf.local_time],
                        commission=0,
                        bar_count=self.last_bar['count']
                    )
                    self.publish(order, plot=True)
                    self.has_position = 'long'
                    self.watch_open = False
                    self.counter += 1

            # open short
            elif self.watch_open == 'short':
                if last_price <= self.last_bar['low']:

                    # make short order event
                    order = OrderEvent(
                        direction=AthenaProperNames.short,
                        subtype='open',
                        quantity=10,
                        contract=message[Tf.contract],
                        price=last_price,
                        update_time=message[Tf.local_time],
                        commission=0,
                        bar_count=self.last_bar['count']
                    )
                    self.publish(order, plot=True)
                    self.has_position = 'short'
                    self.watch_open = False
                    self.counter += 1

            else: return

    def __stoploss_logic(self, message):
        """

        :param message:
        :return:
        """
        if self.closing_signal_list['tracking_high'] is np.nan:
            # does not have trailing stop signal yet
            return

        # otherwise
        last_price = float(message['last_price'])

        # close long
        if self.has_position == 'long':
            if last_price < self.closing_signal_list['tracking_low'] - 1:

                # make short order event (close long)
                order = OrderEvent(
                    direction=AthenaProperNames.short,
                    subtype='stoploss',
                    quantity=10,
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

                self.counter += 1

        elif self.has_position == 'short':
            if last_price > self.closing_signal_list['tracking_high'] + 1:

                # make long order event (close short)
                order = OrderEvent(
                    direction=AthenaProperNames.long,
                    subtype='stoploss',
                    quantity=10,
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

                self.counter += 1

    def __stopwin_logic(self, message):
        """

        :param message:
        :return:
        """
        if self.closing_signal_list['tracking_high'] is np.nan:
            # does not have trailing stop signal yet
            return

        # otherwise
        close_price = message['close']
        # close long
        if self.has_position == 'long':
            low_price = message[Kf.low_price]
            if low_price < self.closing_signal_list['tracking_high'] * (
                        1-self.stop_win_threshold):

                # make short order event (close long)
                order = OrderEvent(
                    direction=AthenaProperNames.short,
                    subtype='stopwin',
                    quantity=10,
                    contract=message[Kf.contract],
                    price=close_price,
                    update_time=message[Kf.end_time],
                    commission=0,
                    bar_count=self.last_bar['count']
                )
                self.publish(order, plot=True)

                self.has_position = False
                self.start_stoploss_logic = False
                self.start_stopwin_logic = False

                self.counter += 1

        elif self.has_position == 'short':
            high_price = message[Kf.high_price]
            if high_price > self.closing_signal_list['tracking_low'] * (
                1+self.stop_win_threshold):

                # make long order event (close short)
                order = OrderEvent(
                    direction=AthenaProperNames.long,
                    subtype='stopwin',
                    quantity=10,
                    contract=message[Kf.contract],
                    price=close_price,
                    update_time=message[Kf.end_time],
                    commission=0,
                    bar_count=self.last_bar['count']
                )
                self.publish(order, plot=True)

                self.has_position = False
                self.start_stoploss_logic = False
                self.start_stopwin_logic = False

                self.counter += 1

    def on_message(self, message):
        """

        :param message:
        :return:
        """
        if message['tag'] == 'don':
            # on donchian signal event
            for field in CTAStrategy1.opening_signal_fields[:4]:
                self.opening_signal_list[field] = float(message[field])

            # if signal list is complete
            if not np.nan in self.opening_signal_list.values():
                # execute watch open logic
                self.__watch_open_logic()

        elif message['tag'] == 'ma':
            # on ma signal event
            for field in CTAStrategy1.opening_signal_fields[4:]:
                self.opening_signal_list[field] = float(
                    message[CTAStrategy1.ma_window_widths[field]])

            # if signal list is complete
            if not np.nan in self.opening_signal_list.values():
                # execute watch open logic
                self.__watch_open_logic()

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
            for field in CTAStrategy1.bar_fields:
                self.last_bar[field] = float(message[field])
            self.last_bar['count'] += 1

        elif message['tag'] == 'stop':
            # on trailing stop signals.
            for field in CTAStrategy1.closing_signal_fields:
                self.closing_signal_list[field] = float(message[field])
