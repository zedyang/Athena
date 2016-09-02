from collections import deque
import multiprocessing as mp
import numpy as np
import time
import pyqtgraph as pg

from Athena.settings import AthenaConfig
from Athena.signals.signal import SignalTemplate
from Athena.data_handler.data_handler import HermesDataHandler

Kf = AthenaConfig.HermesKLineFields


__author__ = 'Atom'


class MarketVolBatch(SignalTemplate):
    """
    Implementation of a batch of Price Moving

    window_widths : Decide Data Length
    target : Decide Data Type - open/high/low/close

    """
    signal_name_prefix = 'signal:market_vol'
    param_names = ['window_widths', 'target']


    def __init__(self, subscribe_list, param_list, duplicate=1):
        """

        """
        super(MarketVolBatch, self).__init__(
            subscribe_list, duplicate
        )

        self._map_to_channels(param_list,
                              suffix=subscribe_list[0].split(':')[-1])

        # set parammeters

        self.tag = 'market_vol'
        self.window_widths = self.param_dict['window_widths']
        self.longest_widths = max(self.window_widths) + 1
        self.targeting_series = self.param_dict['target']

        # initialize market vol  deque as [nan] list.
        self.cached_price = deque(maxlen=self.longest_widths)
        self.cached_price.extend([np.nan] * self.longest_widths)

    def on_message(self, message):
        """
        on receiving a bar message
        :param message:
        :return:
        """
        if message['tag'] == 'kl':
            # print(message[Kf.open_time],)
            self.cached_price.append(message[self.targeting_series])

            print(self.cached_price)

            # publish the market vol by truncating deque at different widths
            to_publish = {
                Kf.open_time: message[Kf.open_time],
                Kf.close_time: message[Kf.close_time],
                Kf.count: message[Kf.count]
            }

            # append market vol for all window widths

            for widths in self.window_widths:
                cached_price_array = np.array(self.cached_price)

                temp_now = cached_price_array[self.longest_widths-widths::]
                temp_his = cached_price_array[self.longest_widths-widths-1:-1]

                temp_pricechange = np.array(temp_now) - np.array(temp_his)
                temp_pricechange_abs = abs(temp_pricechange)
                # print(np.mean(temp_pricechange_abs)/widths)
                to_publish[str(widths)] = np.mean(temp_pricechange_abs)/widths

            self.publish(to_publish)
            # print(to_publish)


def hermes_pub():
    data_handler = HermesDataHandler()
    data_handler.add_instrument('GC1612', ('1m', '3s'))
    data_handler.replay_data(attach_end_flag=True)


def sub_market_vol(instrument):
    signal_market_vol = MarketVolBatch(
        [
            'kl:{}.1m'.format(instrument)
        ],
        param_list=[[5, 4, 3], 'close']
    )
    signal_market_vol.start()


def show_signal():
    win = pg.GraphicsWindow()

    win.setWindowTitle('pyqtgraph example: Scrolling Plots')


if __name__ == '__main__':
    mp.set_start_method('spawn')

    inst = 'GC1612'

    # p_ui = mp.Process(target=show_signal)

    processes = []
    p1 = mp.Process(target=sub_market_vol, args=(inst,))
    # p2 = mp.Process(target=show_signal)
    processes.extend([p1])

    p_pub =mp.Process(target=hermes_pub)

    # p_ui.start()
    time.sleep(1)
    [p.start() for p in processes]

    time.sleep(1)
    p_pub.start()






















