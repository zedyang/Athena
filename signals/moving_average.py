from collections import deque

import numpy as np

from Athena.settings import AthenaConfig
from Athena.signals.signal import SignalTemplate
Kf = AthenaConfig.HermesKLineFields

__author__ = 'zed'


class MovingAverageBatch(SignalTemplate):
    """
    Implementation of a batch of Moving average series of different
    window width.

    The sub_channel should be a kline.
    """
    signal_name_prefix = 'signal:ma'
    param_names = ['window_widths', 'target']

    def __init__(self, subscribe_list, param_list, duplicate=1):
        """

        """
        super(MovingAverageBatch, self).__init__(
            subscribe_list, duplicate
        )
        self._map_to_channels(param_list,
                              suffix=subscribe_list[0].split(':')[-1])

        # set parameters
        self.tag = 'ma'
        self.window_widths = self.param_dict['window_widths']
        self.longest_widths = max(self.window_widths)
        self.targeting_series = self.param_dict['target']

        # initialize moving average deque as [nan] list.
        self.cached_price = deque(maxlen=self.longest_widths)
        self.cached_price.extend([np.nan] * self.longest_widths)

    def on_message(self, message):
        """
        on receiving a bar message.
        :param message:
        :return:
        """
        if message['tag'] == 'kl':

            # append_right the price to the deque. The leftmost item is
            # automatically popped when the maximum length is reached.
            self.cached_price.append(message[self.targeting_series])

            # publish the moving average by truncating deque
            # at different widths.
            to_publish = {
                Kf.open_time: message[Kf.open_time],
                Kf.close_time: message[Kf.close_time],
                Kf.count: message[Kf.count]
            }

            # append ma fields for all window widths
            for width in self.window_widths:
                to_publish[str(width)] = np.mean(
                    list(self.cached_price)[(self.longest_widths-width)::])

            # publish and print
            self.publish(to_publish)

