from collections import deque
from Athena.signals.derived_signal import DerivedSignal

__author__ = 'zed'


class HighestHigh(DerivedSignal):
    """
    Implementation of highest (historical) high signal.
    """
    def __init__(self, sub_channel):
        """
        constructor
        :param sub_channel:
        """
        super(HighestHigh, self).__init__(sub_channel)
        self.signal_name = 'hl'
        self.pub_channel = self.signal_name + '_' + sub_channel

        # initialize highest high and lowest low.
        self.hh = {
            'BID': None,
            'ASK': None
        }
        self.ll = {
            'BID': None,
            'ASK': None
        }

        # record last close time of bar
        self.last_close_time = None

    def on_message(self, message):
        """
        on receiving a bar message. Refer to GeneralBar object.
        bar_data = {
            'START_TIME':
            'END_TIME':
            'ASK_OPEN':
            'ASK_HIGH':
            'ASK_LOW':
            'ASK_CLOSE':
            'BID_OPEN':
            'BID_HIGH':
            'BID_LOW':
            'BID_CLOSE':
            'ASK_VOLUME':
            'BID_VOLUME':
        }
        :param message:
        :return:
        """
        if not self.last_close_time:
            # initialize
            self.last_close_time = message['END_TIME']
            self.hh['ASK'] = message['ASK_HIGH']
            self.hh['BID'] = message['BID_HIGH']
            self.ll['ASK'] = message['ASK_LOW']
            self.ll['BID'] = message['BID_LOW']

        # otherwise
        self.hh['ASK'] = max(self.hh['ASK'], message['ASK_HIGH'])
        self.hh['BID'] = max(self.hh['BID'], message['BID_HIGH'])
        self.ll['ASK'] = min(self.ll['ASK'], message['ASK_LOW'])
        self.ll['BID'] = min(self.ll['BID'], message['BID_LOW'])

        to_publish = {
            'ASK_HIST_HIGH': self.hh['ASK'],
            'BID_HIST_HIGH': self.hh['ASK'],
            'ASK_HIST_LOW': self.ll['ASK'],
            'BID_HIST_LOW': self.ll['BID'],
        }
        self.publish_message(to_publish)

        print('---')
        print(to_publish)

        # increase the counter
        self.counter += 1


class DonchianChannel(DerivedSignal):
    """
    Implementation of donchian channel
    """
    def __init__(self, underlying_signal, n):
        """
        constructor
        :param underlying_signal: string
        """
        super(DonchianChannel, self).__init__(underlying_signal)
        self.signal_name = 'hh_' + underlying_signal

        # highest high and lowest low
        self.cached = deque(n)

    def on_message(self, message):
        """

        :param message:
        :return:
        """
        pass
