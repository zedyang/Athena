from collections import deque

import numpy as np

from Athena.settings import AthenaConfig
from Athena.signals.signal import SignalTemplate
Kf = AthenaConfig.KLineFields

__author__ = 'zed'


class DonchianChannel(SignalTemplate):
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
    def __init__(self, sub_channels, window):
        """

        :param sub_channels:
        :param window:
        """
        super(DonchianChannel, self).__init__(sub_channels)
        self.window = window

        # set signal names and channels.
        self.tag = 'don'
        self.signal_name = 'signal:donchian.' + str(window)
        self.pub_channel = self.signal_name + '.' + self.sub_names[0]
        self.pub_channel_plot = 'plot:' + self.pub_channel

        # initialize high/low prices deque.
        self.cached_prices = {
            Kf.high_price: deque(maxlen=window),
            Kf.low_price: deque(maxlen=window),
        }
        self.cached_prices[Kf.high_price].extend([np.nan] * self.window)
        self.cached_prices[Kf.low_price].extend([np.nan] * self.window)

        # last up/down
        self.last_up = np.nan
        self.last_down = np.nan

    def on_message(self, message):
        """
        on receiving a bar message.
        :param message:
        :return:
        """
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
            Kf.end_time: message[Kf.end_time],
            Kf.count: message[Kf.count],
            'up': np.max(self.cached_prices[Kf.high_price]),
            'down': np.min(self.cached_prices[Kf.low_price]),
            'up_break': max(high - self.last_up, 0),
            'down_break': max(self.last_down - low, 0),
            'up_control': max(close - self.last_up, 0),
            'down_control': max(self.last_down - close, 0)
        }

        # append middle field.
        to_publish['middle'] = (to_publish['up'] + to_publish['down']) / 2

        # update last up and down channels.
        self.last_up = to_publish['up']
        self.last_down = to_publish['down']

        # publish and print
        self._publish(to_publish)
        print('---')
        print(to_publish)

if __name__ == '__main__':
    d = DonchianChannel(['kl:GC1608.1m'],20)
    d.start()