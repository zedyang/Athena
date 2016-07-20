from collections import deque

import numpy as np

from Athena.settings import AthenaConfig
from Athena.signals.signal import SignalTemplate
Kf = AthenaConfig.KLineFields

__author__ = 'zed'


class MovingAverageBatch(SignalTemplate):
    """
    Implementation of a batch of Moving average series of different
    window width.

    The sub_channel should be a kline.
    """
    def __init__(self, sub_channels, window_widths, target='close'):
        """
        constructor.
        :param sub_channels:
        :param window_widths: list of integers,
            the width of moving average window.
        :param target: which series in ohlc to calculate ma.
        """
        super(MovingAverageBatch, self).__init__(sub_channels)
        self.window_widths = window_widths
        self.longest_widths = max(self.window_widths)

        # set signal names and channels.
        self.tag = 'ma'
        self.signal_name = 'signal:ma'
        self.pub_channel = self.signal_name + '.' + self.sub_names[0]
        self.pub_channel_plot = 'plot:' + self.pub_channel

        # initialize moving average deque as [nan] list.
        self.cached_price = deque(maxlen=self.longest_widths)
        self.cached_price.extend([np.nan] * self.longest_widths)

        self.targeting_series = target

    def on_message(self, message):
        """
        on receiving a bar message.
        :param message:
        :return:
        """
        # append_right the price to the deque. The leftmost item is
        # automatically popped when the maximum length is reached.
        self.cached_price.append(message[self.targeting_series])

        # publish the moving average by truncating deque at different widths.
        to_publish = {
            Kf.open_time: message[Kf.open_time],
            Kf.end_time: message[Kf.end_time],
            Kf.count: message[Kf.count]
        }

        # append ma fields for all window widths
        for width in self.window_widths:
            to_publish[str(width)] = np.mean(
                list(self.cached_price)[(self.longest_widths-width)::])

        # publish and print
        self._publish(to_publish)
        print('---')
        print(to_publish)
