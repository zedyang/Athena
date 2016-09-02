import numpy as np

from Athena.settings import AthenaConfig
from Athena.signals.signal import SignalTemplate
Kf = AthenaConfig.HermesKLineFields

__author__ = 'zed'


class TrailingStop(SignalTemplate):
    """
    Implementation of trailing stop loss.
    Keeps track on positions and k-lines, begin to publish when there is
    opened position. Updated on k-line data.
    """
    signal_name_prefix = 'signal:trailing.stop'
    param_names = ['instrument', 'target_strategy']

    def __init__(self, subscribe_list, param_list):
        """

        :param subscribe_list:
        :param param_list:
        """
        super(TrailingStop, self).__init__(subscribe_list)
        self._map_to_channels(param_list,
                              suffix=param_list[0])

        # set parameters
        self.tag = 'stop'
        self.target_strategy = self.param_dict['target_strategy']
        self.is_tracking = False

        # signal list
        self.signal_list = {
            'tracking_high': np.nan,
            'tracking_low': np.nan,
        }

    def __tracking(self, message):
        """
        tracking logic
        :param message:
        :return:
        """
        if not self.is_tracking:
            # is not tracking
            pass

        else:

            if self.signal_list['tracking_high'] is np.nan:

                # if it is the first bar after tracking
                self.signal_list['tracking_high'] = \
                    message[Kf.high_price]
                self.signal_list['tracking_low'] = \
                    message[Kf.high_price]

            else:
                high, low = message[Kf.high_price], message[Kf.low_price]

                # if it is not the first bar
                # tracking long position
                if self.is_tracking == 'long':

                    # update stop loss
                    if high > self.signal_list['tracking_high']:

                        self.signal_list['tracking_high'] = high
                        self.signal_list['tracking_low'] = low

                # tracking short position
                elif self.is_tracking == 'short':

                    # update stop loss
                    if low < self.signal_list['tracking_low']:

                        self.signal_list['tracking_high'] = high
                        self.signal_list['tracking_low'] = low

        # make signal
        to_publish = {
            Kf.open_time: message[Kf.open_time],
            Kf.contract: message[Kf.contract],
            Kf.count: message[Kf.count],
            'tracking_high': self.signal_list['tracking_high'],
            'tracking_low': self.signal_list['tracking_low'],
            'direction': self.is_tracking
        }

        # publish and print
        self.publish(to_publish)

    def __change_status(self, message):
        """

        :param message:
        :return:
        """
        if not self.is_tracking:

            # start tracking
            self.is_tracking = message['direction']

        else:

            # end tracking
            self.is_tracking = False

            # reset signal list
            self.signal_list['tracking_high'] = np.nan
            self.signal_list['tracking_low'] = np.nan

    def on_message(self, message):
        """
        on receiving a bar message.
        :param message:
        :return:
        """
        if message['tag'] == 'kl':

            # on kline message
            self.__tracking(message)

        elif message['tag'] == self.target_strategy:

            # on buy/sell signals
            self.__change_status(message)
