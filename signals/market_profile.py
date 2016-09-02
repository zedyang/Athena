from itertools import groupby

import numpy as np

from Athena.settings import AthenaConfig
from Athena.signals.signal import SignalTemplate
Kf = AthenaConfig.HermesKLineFields

__author__ = 'zed'


class MarketProfile(SignalTemplate):
    """
    Implementation of a batch of Moving average series of different
    window width.

    The sub_channel should be a kline.
    """
    signal_name_prefix = 'signal:mp'
    param_names = ['va_proportion', 'cycle', 'step_size']

    def __init__(self, subscribe_list, param_list, duplicate=1):
        """

        """
        super(MarketProfile, self).__init__(
            subscribe_list, duplicate
        )
        self._map_to_channels(param_list,
                              suffix=subscribe_list[0].split(':')[-1])

        # set parameters
        self.tag = 'mp'
        self.va_proportion = self.param_dict['va_proportion']
        self.period = self.param_dict['cycle']
        self.step_size = self.param_dict['step_size']

        self.TPOs = []  # time price opportunities
        self.count_for_this_prd = 0
        self.open_bar_count = None

    def on_message(self, message):
        """
        on receiving a bar message.
        :param message:
        :return:
        """
        # open a period
        if not self.TPOs:
            self.open_bar_count = message[Kf.count]

        # range of a kline
        high_price = message[Kf.high_price]
        low_price = message[Kf.low_price]

        # number of ticks' steps
        steps = (high_price - low_price) / self.step_size + 1
        steps = int(round(steps, 0))

        # Time price opportunities within this kline
        tpo = [str(x) for x in np.linspace(low_price, high_price, steps)]
        self.TPOs.extend(tpo)
        self.TPOs = sorted(self.TPOs)

        # make range and counts
        tp_range = sorted(list(set(self.TPOs)))
        tp_counts = [len(list(group)) for k, group in groupby(self.TPOs)]
        distribution = list(zip(tp_range, tp_counts))

        # calculate POC, VA
        poc = sorted(distribution, key=lambda x: x[1])[-1][0]
        poc_ind = tp_range.index(poc)

        va_tpo_counts = int(self.va_proportion * sum(tp_counts)) \
                        - tp_counts[poc_ind]

        vah_ind = poc_ind
        val_ind = poc_ind
        while va_tpo_counts > 0:
            try:
                if (
                    tp_counts[vah_ind+1] > tp_counts[val_ind-1]
                ) or (
                    val_ind == 0
                ):
                    vah_ind += 1
                    va_tpo_counts -= tp_counts[vah_ind]
                else:
                    val_ind -= 1
                    va_tpo_counts -= tp_counts[val_ind]
            except IndexError:
                break

        to_publish = {
            'open_bar_count': self.open_bar_count,
            'this_bar_count': message[Kf.count],
            'range': str(tp_range),
            'counts': str(tp_counts),
            'poc_index': poc_ind,
            'val_index': val_ind,
            'vah_index': vah_ind,
            'poc': tp_range[poc_ind],
            'val': tp_range[val_ind],
            'vah': tp_range[vah_ind]
        }

        # publish and print
        self.publish(to_publish)

        # update periodic counter
        self.count_for_this_prd += 1

        # if reached a full period, reset and go into next generating period.
        if self.count_for_this_prd == self.period:
            self.TPOs = []
            self.count_for_this_prd = 0
