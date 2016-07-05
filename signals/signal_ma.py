from collections import deque

import numpy as np

from Athena.signals.derived_signal import DerivedSignal

__author__ = 'zed'


class MovingAverage(DerivedSignal):
    """
    Implementation of Moving average.
    """
    def __init__(self, sub_channel, window):
        """
        constructor.
        :param sub_channel:
        :param window: integer, the width of moving average window.
        """
        super(MovingAverage, self).__init__(sub_channel)
        self.window = window
        self.signal_name = 'ma_' + str(window)
        self.pub_channel = self.signal_name + '_' + sub_channel

        # initialize moving average deque.
        self.cached_price = {
            'ASK': deque(maxlen=window),  # ask
            'BID': deque(maxlen=window),
        }

        self.targeting_series = 'CLOSE'

    def on_message(self, message):
        """
        on receiving a bar message.
        :param message:
        :return:
        """
        # append_right the price to the deque. The leftmost item is
        # automatically popped when the maximum length is reached.
        self.cached_price['BID'].append(
            message['BID_' + self.targeting_series])
        self.cached_price['ASK'].append(
            message['ASK_' + self.targeting_series])

        # If reached max length, publish an MA message
        if len(self.cached_price['BID']) >= self.window:
            to_publish = {
                'ASK': np.mean(self.cached_price['ASK']),
                'BID': np.mean(self.cached_price['BID'])
            }
            self.publish_message(to_publish)

            print('---')
            print(to_publish)

            # increment on the counter
            self.counter += 1