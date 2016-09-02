from datetime import datetime, timedelta
import numpy as np

from Athena.settings import AthenaConfig
from Athena.containers import OrderEvent, OrderType
from Athena.strategies.strategy import StrategyTemplate

Tf, Kf = AthenaConfig.HermesTickFields, AthenaConfig.HermesKLineFields

__author__ = 'zed'


class CTAStrategy2(StrategyTemplate):
    """

    """
    strategy_name_prefix = 'strategy:cta_2'
    param_names = ['ma', 'don_up', 'don_down', 'stop_win', 'trailing']

    def __init__(self, subscribe_list, param_list,
                 instrument, train=False):
        """

        :param subscribe_list:
        :param param_list:
        :param instrument:
        :param train:
        """
        super(CTAStrategy2, self).__init__(subscribe_list)
        self._map_to_channels(param_list, suffix=instrument,
                              full_name=train)

        # set parameters

        # containers
        self.watch_open_signal_list = {
            'ma': np.nan
        }

        self.hh, self.ll = 'up_'+str(self.param_dict['don_up']), \
                           'down_'+str(self.param_dict['don_down'])

        self.open_signal_list = {
            self.hh: np.nan,
            self.ll: np.nan,
            'count': -1
        }

        self.stop_win_signal_list = {
            'entry_price': np.nan,
            'max_payoff_price': np.nan
        }

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
        self.has_position = False
        self.watch_open = False
        self.watch_cover = False
        self.start_stopwin_logic = False

    @staticmethod
    def __assert_time(dt, debug=True):
        """

        :param dt:
        :return:
        """
        if debug:   # if we don't care about time
            return True

        dt = datetime.strptime(dt, AthenaConfig.dt_format)
        day_start = datetime(dt.year, dt.month, dt.day)
        if day_start + timedelta(hours=9, minutes=30) < dt \
                <= day_start + timedelta(hours=14):
            return True
        else: return False

    @staticmethod
    def __assert_condition_1(dt, close, ma, has_position):
        a = CTAStrategy2.__assert_time(dt)
        b = (close > ma)
        c = (has_position is False) or (has_position == 'short')
        return a and b and c

    @staticmethod
    def __assert_condition_2(dt, close, ma, has_position):
        a = CTAStrategy2.__assert_time(dt)
        b = (close < ma)
        c = (has_position is False) or (has_position == 'long')
        return a and b and c

    def __watch_open_logic(self, message):
        """

        :return:
        """
        ma = self.watch_open_signal_list['ma']
        close, bar_end_time = \
            self.last_bar[Kf.close_price], self.last_bar[Kf.close_time]

        # enter watch buy
        if CTAStrategy2.__assert_condition_1(
                bar_end_time, close, ma, self.has_position):
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

        # condition 2 (watch sell)
        elif CTAStrategy2.__assert_condition_2(
                bar_end_time, close, ma, self.has_position):
            # make change status event
            order = OrderEvent(
                direction=None,
                type=None,
                subtype='watch_sell',
                quantity=0,
                contract=None,
                price=np.nan,
                update_time=self.last_bar[Kf.close_time],
                commission=0,
                bar_count=self.last_bar['count']
            )
            self.publish(order, plot=True)

            self.watch_open = 'short'

        # (watch cover short)
        elif not CTAStrategy2.__assert_condition_1(
                bar_end_time, close, ma, self.has_position) \
                and self.has_position == 'short':
            # make change status event
            order = OrderEvent(
                direction=None,
                type=None,
                subtype='watch_cover_short',
                quantity=0,
                contract=None,
                price=np.nan,
                update_time=self.last_bar[Kf.close_time],
                commission=0,
                bar_count=self.last_bar['count']
            )
            self.publish(order, plot=True)

            self.watch_cover = 'short'

        # (watch cover long)
        elif not CTAStrategy2.__assert_condition_2(
                bar_end_time, close, ma, self.has_position) \
                and self.has_position == 'long':
            # make change status event
            order = OrderEvent(
                direction=None,
                type=None,
                subtype='watch_cover_long',
                quantity=0,
                contract=None,
                price=np.nan,
                update_time=self.last_bar[Kf.close_time],
                commission=0,
                bar_count=self.last_bar['count']
            )
            self.publish(order, plot=True)

            self.watch_cover = 'long'

        else:
            # otherwise, exit watch open status, exit watch cover status
            if self.watch_open:
                # make change status event
                order = OrderEvent(
                    direction=None,
                    type=None,
                    subtype='watch_open_exit',
                    quantity=0,
                    contract=self.last_bar[Kf.contract],
                    price=np.nan,
                    update_time=self.last_bar[Kf.close_time],
                    commission=0,
                    bar_count=self.last_bar['count']
                )
                self.publish(order, plot=True)

            if self.watch_cover:
                # make change status event
                order = OrderEvent(
                    direction=None,
                    type=None,
                    subtype='watch_cover_exit',
                    quantity=0,
                    contract=self.last_bar[Kf.contract],
                    price=np.nan,
                    update_time=self.last_bar[Kf.close_time],
                    commission=0,
                    bar_count=self.last_bar['count']
                )
                self.publish(order, plot=True)

            self.watch_open = False
            self.watch_cover = False

    def __open_logic(self, message):
        """

        :param message:
        :return:
        """
        # if donchian is updated on this bar
        if self.open_signal_list[Kf.count] == self.last_bar[Kf.count]:
            last_price = float(message[Tf.last_price])

            # open long (cover short if any)
            if (
                self.watch_open == 'long'
            ) and (
                last_price >= self.open_signal_list[self.hh]
            ):
                if self.has_position == 'short':
                    # if has a short, cover first
                    order = OrderEvent(
                        direction='long',
                        type=OrderType.cover_short,
                        subtype='cover_short',
                        quantity=1,
                        contract=message[Tf.contract],
                        price=last_price,
                        update_time=message[Tf.local_time],
                        commission=0,
                        bar_count=self.last_bar[Kf.count]+1
                    )
                    self.publish(order, plot=True)
                    self.has_position = False

                # then, open a long position
                order = OrderEvent(
                    direction='long',
                    type=OrderType.open_long,
                    subtype='open_long',
                    quantity=1,
                    contract=message[Tf.contract],
                    price=last_price,
                    update_time=message[Tf.local_time],
                    commission=0,
                    bar_count=self.last_bar[Kf.count]+1
                )
                self.publish(order, plot=True)
                self.has_position = 'long'
                self.watch_open = False

                # reset stop win
                for k in self.stop_win_signal_list:
                    self.stop_win_signal_list[k] = last_price

            # open short (cover long if any)
            elif (
                self.watch_open == 'short'
            ) and (
                last_price <= self.open_signal_list[self.ll]
            ):
                if self.has_position == 'long':
                    # if has a long, cover first
                    order = OrderEvent(
                        direction='short',
                        type=OrderType.cover_long,
                        subtype='cover_long',
                        quantity=1,
                        contract=message[Tf.contract],
                        price=last_price,
                        update_time=message[Tf.local_time],
                        commission=0,
                        bar_count=self.last_bar[Kf.count]+1
                    )
                    self.publish(order, plot=True)
                    self.has_position = False

                # then, open a short position
                order = OrderEvent(
                    direction='short',
                    type=OrderType.open_short,
                    subtype='open_short',
                    quantity=1,
                    contract=message[Tf.contract],
                    price=last_price,
                    update_time=message[Tf.local_time],
                    commission=0,
                    bar_count=self.last_bar[Kf.count]+1
                )
                self.publish(order, plot=True)
                self.has_position = 'short'
                self.watch_open = False

                # reset stop win
                for k in self.stop_win_signal_list:
                    self.stop_win_signal_list[k] = last_price

            else:
                return

    def __cover_logic(self, message):
        """

        :param message:
        :return:
        """
        # if donchian is updated on this bar
        if self.open_signal_list[Kf.count] == self.last_bar[Kf.count]:
            last_price = float(message[Tf.last_price])

            # cover short
            if (
                self.watch_cover == 'short'
            ) and (
                last_price >= self.open_signal_list[self.hh]
            ):

                if self.has_position == 'short':
                    order = OrderEvent(
                        direction='long',
                        type=OrderType.cover_short,
                        subtype='cover_short',
                        quantity=1,
                        contract=message[Tf.contract],
                        price=last_price,
                        update_time=message[Tf.local_time],
                        commission=0,
                        bar_count=self.last_bar[Kf.count]
                    )
                    self.publish(order, plot=True)
                    self.has_position = False

                    # clear stop win
                    for k in self.stop_win_signal_list:
                        self.stop_win_signal_list[k] = np.nan
                    self.start_stopwin_logic = False

            # cover long
            elif (
                self.watch_cover == 'long'
            ) and (
                last_price <= self.open_signal_list[self.ll]
            ):

                if self.has_position == 'long':
                    order = OrderEvent(
                        direction='short',
                        type=OrderType.cover_long,
                        subtype='cover_long',
                        quantity=1,
                        contract=message[Tf.contract],
                        price=last_price,
                        update_time=message[Tf.local_time],
                        commission=0,
                        bar_count=self.last_bar[Kf.count]
                    )
                    self.publish(order, plot=True)
                    self.has_position = False

                    # clear stop win
                    for k in self.stop_win_signal_list:
                        self.stop_win_signal_list[k] = np.nan
                    self.start_stopwin_logic = False

            else:
                return

    def __stopwin_logic(self, message):
        """

        :param message:
        :return:
        """
        last_price = float(message[Tf.last_price])

        # long position stopwin
        if self.has_position == 'long':
            # update max payoff
            self.stop_win_signal_list['max_payoff_price'] = max(
                self.stop_win_signal_list['max_payoff_price'],
                last_price
            )

            # stop win on drawback from max payoff
            drawback = \
                self.stop_win_signal_list['max_payoff_price'] - last_price
            if drawback > max(
                self.param_dict['trailing'] / 100 *
                self.stop_win_signal_list['entry_price'],
                self.param_dict['stop_win']
            ):
                order = OrderEvent(
                    direction='short',
                    type=OrderType.cover_long,
                    subtype='stop_win',
                    quantity=1,
                    contract=message[Tf.contract],
                    price=last_price,
                    update_time=message[Tf.local_time],
                    commission=0,
                    bar_count=self.last_bar[Kf.count]
                )
                self.publish(order, plot=True)
                self.has_position = False

                # clear stop win
                for k in self.stop_win_signal_list:
                    self.stop_win_signal_list[k] = np.nan
                self.start_stopwin_logic = False

        # short position stop win
        elif self.has_position == 'short':

            # update max payoff
            self.stop_win_signal_list['max_payoff_price'] = min(
                self.stop_win_signal_list['max_payoff_price'],
                last_price
            )

            # stop win on drawback from max payoff
            drawback = \
                last_price - self.stop_win_signal_list['max_payoff_price']
            if drawback > max(
                self.param_dict['trailing'] / 100 *
                self.stop_win_signal_list['entry_price'],
                self.param_dict['stop_win']
            ):
                order = OrderEvent(
                    direction='long',
                    type=OrderType.cover_short,
                    subtype='stop_win',
                    quantity=1,
                    contract=message[Tf.contract],
                    price=last_price,
                    update_time=message[Tf.local_time],
                    commission=0,
                    bar_count=self.last_bar[Kf.count]
                )
                self.publish(order, plot=True)
                self.has_position = False

                # clear stop win
                for k in self.stop_win_signal_list:
                    self.stop_win_signal_list[k] = np.nan
                self.start_stopwin_logic = False

        else:
            return

    def on_message(self, message):
        """

        :param message:
        :return:
        """
        if message['tag'] == 'don':
            # on donchian signal event
            for field in self.open_signal_list:
                self.open_signal_list[field] = message[field]

        elif message['tag'] == 'ma':
            # on ma signal event
            self.watch_open_signal_list['ma'] = \
                message[str(self.param_dict['ma'])]

            # watch open
            self.__watch_open_logic(message)

        elif message['tag'] == 'md':
            # on ticks
            # open on ticks
            self.__stopwin_logic(message)
            self.__open_logic(message)
            self.__cover_logic(message)

        elif message['tag'] == 'kl':
            # on bars
            # stop win is opened on next bar
            if self.has_position:
                self.start_stopwin_logic = True

            # update last bar
            for field in Kf.ohlc:
                self.last_bar[field] = float(message[field])
            self.last_bar[Kf.count] = message[Kf.count]
            self.last_bar[Kf.close_time] = message[Kf.close_time]
            self.last_bar[Kf.contract] = message[Kf.contract]
