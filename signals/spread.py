from collections import deque

import numpy as np

from Athena.settings import AthenaConfig
from Athena.signals.signal import SignalTemplate
Tf, Kf = AthenaConfig.HermesTickFields, AthenaConfig.HermesKLineFields

__author__ = 'zed'


class FutureSpotSpread(SignalTemplate):
    """
    sub_channels should be tick and k-lines of spot, future contracts
    """
    signal_name_prefix = 'signal:spread'
    param_names = ['legs', 'cycle']

    def __init__(self, subscribe_list, param_list, duplicate=1):
        """

        :param subscribe_list:
        :param param_list:
        """
        super(FutureSpotSpread, self).__init__(
            subscribe_list, duplicate
        )
        self._map_to_channels(param_list,
                              suffix='.'.join(param_list[0]))

        # set parameters
        self.tag = 'spread'
        self.legs = self.param_dict['legs']
        self.spot_leg = self.legs[0]
        self.future_leg = self.legs[1]
        self.re_estimate_cycle = self.param_dict['cycle']

        # containers
        self.prices_bid = {
            self.spot_leg: np.nan,
            self.future_leg: np.nan
        }

        self.prices_ask = {
            self.spot_leg: np.nan,
            self.future_leg: np.nan
        }

        # cached spread
        self.mean_buy, self.mean_sell = np.nan, np.nan
        self.band_mean = np.nan
        self.cached_spread_buy = deque(maxlen=self.re_estimate_cycle)
        self.cached_spread_sell = deque(maxlen=self.re_estimate_cycle)
        self.cached_spread_buy.extend(
            [np.nan] * self.re_estimate_cycle)
        self.cached_spread_sell.extend(
            [np.nan] * self.re_estimate_cycle)

        self.re_estimate_cycle_counter = 0

    def on_message(self, message):
        """

        :param message:
        :return:
        """
        # update prices on tick
        if message['tag'] == 'md':
            self.prices_bid[message[Tf.contract]] = \
                float(message[Tf.bid_1])
            self.prices_ask[message[Tf.contract]] = \
                float(message[Tf.ask_1])

        # on bar
        if message['tag'] == 'kl':
            spot_bid = self.prices_bid[self.spot_leg]
            future_bid = self.prices_bid[self.future_leg]
            spot_ask = self.prices_ask[self.spot_leg]
            future_ask = self.prices_ask[self.future_leg]

            # update spread on future kline
            if message[Kf.contract] == self.future_leg:

                # append to cached data
                self.cached_spread_buy.append(future_ask - spot_bid)
                self.cached_spread_sell.append(future_bid - spot_ask)

                # re-estimate the mean
                if not (
                    self.re_estimate_cycle_counter % self.re_estimate_cycle
                ):
                    self.mean_buy = np.mean(list(self.cached_spread_buy))
                    self.mean_sell = np.mean(list(self.cached_spread_sell))
                    self.band_mean = (self.mean_buy + self.mean_sell) / 2

                to_publish = {
                    self.spot_leg: (spot_bid + spot_ask) / 2,
                    self.future_leg: (future_bid + future_ask) / 2,
                    'spread_buy': future_ask - spot_bid,
                    'spread_sell': future_bid - spot_ask,
                    Kf.close_time: message[Kf.close_time],
                    Kf.count: message[Kf.count],
                    'spot_leg': self.spot_leg,
                    'future_leg': self.future_leg,
                    'band_mean': self.band_mean
                }

                # publish and print
                self.publish(to_publish)
                self.re_estimate_cycle_counter += 1
