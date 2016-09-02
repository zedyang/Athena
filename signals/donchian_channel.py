from collections import deque

import numpy as np

from Athena.settings import AthenaConfig
from Athena.signals.signal import SignalTemplate
Kf = AthenaConfig.HermesKLineFields

__author__ = 'zed'


class DonchianChannelBatch(SignalTemplate):
    """
    Implementation of Donchian channel with a threshold that avoids
    breaking through too much.

    Signal Algorithm:
    Denote HH := max{high, 20}; LL := min{low, 20}
    up_break_t = high_t - HH_{t-1}
    down_break_t = LL_{t-1} - low_t
    up_control_t = close_t - HH_{t-1}
    down_control_t = LL_{t-1} - close_t

    Open long position at up_break_t > 0, up_control <= threshold
    """
    signal_name_prefix = 'signal:donchian'
    param_names = ['window_widths']

    def __init__(self, subscribe_list, param_list, duplicate=1):
        """

        :param subscribe_list:
        :param param_list:
        """
        if not len(subscribe_list) == 1 and 'kl' in subscribe_list[0]:
            raise ValueError

        super(DonchianChannelBatch, self).__init__(
            subscribe_list, duplicate
        )
        self._map_to_channels(param_list,
                              suffix=subscribe_list[0].split(':')[-1])

        # set parameters
        self.tag = 'don'
        self.window_widths = self.param_dict['window_widths']
        self.longest_width = max(self.window_widths)

        # initialize high/low prices deque.
        self.cached_prices = {
            Kf.high_price: deque(maxlen=self.longest_width),
            Kf.low_price: deque(maxlen=self.longest_width),
        }
        self.cached_prices[Kf.high_price].extend(
            [np.nan] * self.longest_width)
        self.cached_prices[Kf.low_price].extend(
            [np.nan] * self.longest_width)

        # last up/down
        self.last_up = dict()
        self.last_down = dict()
        for width in self.window_widths:
            self.last_up[str(width)] = np.nan
            self.last_down[str(width)] = np.nan

    def on_message(self, message):
        """
        on receiving a bar message.
        :param message:
        :return:
        """
        if message['tag'] == 'kl':
            (high, low, close) = (
                float(message[Kf.high_price]),
                float(message[Kf.low_price]),
                float(message[Kf.close_price])
            )

            # append values.
            self.cached_prices[Kf.high_price].append(high)
            self.cached_prices[Kf.low_price].append(low)

            # make donchian channel data dict.
            to_publish = {
                Kf.open_time: message[Kf.open_time],
                Kf.close_time: message[Kf.close_time],
                Kf.count: message[Kf.count],
            }

            for width in self.window_widths:

                # up and down fields
                to_publish['up_'+str(width)] = np.max(
                    list(self.cached_prices[Kf.high_price])
                    [(self.longest_width-width)::]
                )
                to_publish['down_'+str(width)] = np.min(
                    list(self.cached_prices[Kf.low_price])
                    [(self.longest_width - width)::]
                )

                # break and control fields
                to_publish['up_break_'+str(width)] = max(
                    high - self.last_up[str(width)], 0
                )
                to_publish['down_break_'+str(width)] = max(
                    self.last_down[str(width)] - low, 0
                )
                to_publish['up_control_'+str(width)] = max(
                    close - self.last_up[str(width)], 0
                )
                to_publish['down_control_'+str(width)] = max(
                    self.last_down[str(width)] - close, 0
                )

                # middle field
                to_publish['middle_'+str(width)] = \
                    (to_publish['up_'+str(width)] +
                     to_publish['down_'+str(width)]) / 2

                # update last up and down
                self.last_up[str(width)] = to_publish['up_'+str(width)]
                self.last_down[str(width)] = to_publish['down_'+str(width)]

            # publish and print
            self.publish(to_publish)
