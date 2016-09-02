import numpy as np

from Athena.settings import AthenaConfig
from Athena.signals.signal import SignalTemplate
from Athena.containers import OrderType
Of, Kf = AthenaConfig.OrderFields, AthenaConfig.HermesKLineFields

__author__ = 'zed'


class SpreadStopWin(SignalTemplate):
    """

    """
    signal_name_prefix = 'signal:spread.stop'
    param_names = ['legs', 'stop_level', 'target_strategy', 'train_params']

    def __init__(self, sub_channels, param_list):
        """

        :param sub_channels:
        :param param_list:
        """
        super(SpreadStopWin, self).__init__(sub_channels)
        if param_list[3]:
            suffix = '.'.join(param_list[0]) + '.' + str(param_list[3])
        else:
            suffix = '.'.join(param_list[0])
        self._map_to_channels(param_list,
                              suffix=suffix)

        # set parameters
        self.tag = 'stop'

        self.legs = self.param_dict['legs']
        self.target_strategy = self.param_dict['target_strategy']
        self.spot_leg = self.legs[0]
        self.future_leg = self.legs[1]
        self.stopwin_level = self.param_dict['stop_level']

        # whether the signal is tracking
        self.is_tracking = False

        # position list
        self.positions_list = []
        self.broken_leg = dict()
        self.broken_leg_covered = 0

    def __update_payoffs(self, message):
        """

        :param message:
        :return:
        """
        if not self.positions_list: return

        for t in self.positions_list:
            if t[2] == 'long':
                t[0] = float(message['spread']) - t[1]
            elif t[2] == 'short':
                t[0] = t[1] - float(message['spread'])

        # sort by payoffs
        self.positions_list = sorted(
            self.positions_list,
            key = lambda t: t[0]
        )[::-1]

    def __update_position_list(self, message):

        # if open
        if message[Of.type] in OrderType.open:

            # broken leg
            if not self.broken_leg:

                self.broken_leg[message[Of.contract]] = \
                    float(message[Of.price])
                self.broken_leg['bar_count'] = message[Of.bar_count]

            elif (
                len(self.broken_leg) == 2
            ) and (
                message[Of.bar_count] == self.broken_leg['bar_count']
            ):
                self.broken_leg[message[Of.contract]] \
                    = float(message[Of.price])

                # append to position list
                open_spread = \
                    self.broken_leg[self.future_leg] - \
                    self.broken_leg[self.spot_leg]
                direction \
                    = 'short' \
                    if message[Of.type] == OrderType.open_short \
                    else 'long'

                self.positions_list.append(
                    # (payoff, open_spread, direction)
                    [np.nan, open_spread, direction]
                )

                # renew broken leg cache
                self.broken_leg = dict()

        elif message[Of.type] in OrderType.cover:

            # clean up position list on cover
            self.broken_leg_covered += 1

            if self.broken_leg_covered == 2:
                try:
                    del self.positions_list[0]
                except IndexError:
                    print(message)
                self.broken_leg_covered = 0

    def __send_stopwin(self, message):
        """

        :param message:
        :return:
        """
        stopwin_positions = [
            t for t in self.positions_list if t[0] >= self.stopwin_level
        ]
        quantity = len(stopwin_positions)

        # set direction
        if quantity > 0:
            if stopwin_positions[0][2] == 'long':
                direction = 'short'
            elif stopwin_positions[0][2] == 'short':
                direction = 'long'
            else:
                raise ValueError
        else:
            direction = None

        # make signal
        to_publish = {
            Kf.close_time: message[Kf.close_time],
            Kf.count: message[Kf.count],
            self.spot_leg: message[self.spot_leg],
            self.future_leg: message[self.future_leg],
            'spot_leg': self.spot_leg,
            'future_leg': self.future_leg,
            'quantity': quantity,
            'direction': direction
        }

        # publish and print
        self.publish(to_publish)

    def on_message(self, message):
        """
        on receiving a bar message.
        :param message:
        :return:
        """
        if message['tag'] == 'spread':

            # on kline message
            self.__update_payoffs(message)

            # stopwin
            self.__send_stopwin(message)

        elif message['tag'] == self.target_strategy:

            # on buy/sell orders
            self.__update_position_list(message)


