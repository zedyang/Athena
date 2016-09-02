from collections import deque
import multiprocessing as mp
import numpy as np
import time
import pyqtgraph as pg
from pyqtgraph import QtGui
from Athena.graphics_items.athena_window_new import AthenaMainWindowController
from Athena.settings import AthenaConfig
from Athena.signals.signal import SignalTemplate
from Athena.data_handler.data_handler import HermesDataHandler
#from Athena.signals.count_vol_on_window import CountVolOnWindowBatch
#from Athena.signals.count_vol_on_tick import CountVolOnTickBatch

#Kf = AthenaConfig.KLineFields
Tf = AthenaConfig.TickFields

__author__ = 'Nick'


class CountDeltaVolBatch(SignalTemplate):
    """
    Implementation of a batch of Price Moving

    window_widths : Decide Data Length
    target : Decide Data Type - open/high/low/close

    """
    signal_name_prefix = 'signal:count_delta_vol'

    param_names = ['target']

    def __init__(self, subscribe_list, param_list,duplicate=1):
        """

        """
        super(CountDeltaVolBatch, self).__init__(subscribe_list,duplicate)

        self._map_to_channels(param_list,
                              suffix=subscribe_list[0].split(':')[-1])

        # set parameters

        self.tag = 'count_delta_vol'
        #self.targeting_series = self.param_dict['target']

        # initialize market vol  deque as [nan] list.
        self.cached_price = deque(maxlen=2)
        self.cached_volume = deque(maxlen=2)
        self.cached_ask1 = deque(maxlen=2)
        self.cached_bid1 = deque(maxlen=2)
        self.temp_tradedelta = 0

        self.cached_price.extend([np.nan] * 2)
        self.cached_volume.extend([np.nan] * 2)
        self.cached_ask1.extend([np.nan] * 2)
        self.cached_bid1.extend([np.nan] * 2)

        self.targeting_series = self.param_dict['target']
        a=1

    def on_message(self, message):
        """
        on receiving a bar message
        :param message:
        :return:
        """
        '''
        1st change + message = md
        '''

        if message['tag'] == 'md':
            '''
            2nd change self.targeting_series = self.param_dict['target']
            but the definiation is wrong
            param_names = ['target'] doesn't match p1 = mp.Process(target=CountDeltaVol_sub,
            args=(inst,[['last_price', 'volume', 'bid_vol', 'ask_vol']],))
            '''
            #  *****self.cached_price.append(message[self.targeting_series])
            self.cached_price.append(message['last_price'])


            to_publish = {
                Tf.last_price: message[Tf.last_price],
                Tf.local_time: message[Tf.local_time],
                Tf.ask: message[Tf.ask],
                Tf.bid: message[Tf.bid],

            }

            print (to_publish)

            # append ma fields for all window widths
            trade_volume = self.cached_volume[-1] - self.cached_volume[-2]
            ask_trade = 0
            bid_trade = 0
            if trade_volume > 0:
                self.temp_tradedelta = trade_volume
                if self.cached_price[-1] == self.cached_ask1[-1]:
                    ask_trade = trade_volume
                elif self.cached_price[-1] == self.cached_bid1[-1]:
                    bid_trade = trade_volume
                else:
                    print('something is wrong')
            else:
                self.temp_tradedelta = 0

            to_publish[str('trade_delta')] = self.temp_tradedelta
            to_publish[str('previous_volume')] = self.cached_volume[-2]
            to_publish[str('current_volume')] = self.cached_volume[-1]
            to_publish[str('num_ask_trade')] = ask_trade
            to_publish[str('num_bid_trade')] = bid_trade

            self.publish(to_publish)


class CountDeltaVolWindowBatch(SignalTemplate):
    """
    Implementation of a batch of Price Moving

    window_widths : Decide Data Length
    target : Decide Data Type - open/high/low/close

    """
    signal_name_prefix = 'signal:count_delta_vol_window'

    param_names = ['window_widths', ]

    def __init__(self, subscribe_list, param_list, duplicate=1, num_ask='num_ask_trade',num_bid='num_bid_trade'):
        """

        """
        super(CountDeltaVolWindowBatch, self).__init__(
            subscribe_list, duplicate
        )

        self._map_to_channels(param_list,
                              suffix=subscribe_list[0].split(':')[-1])

        # set parameters

        self.tag = 'count_delta_vol_window'
        self.window_widths = self.param_dict['window_widths']
        self.biggest_window = max(self.window_widths)
        self.targeting_series = self.param_dict['target']

        # initialize market vol  deque as [nan] list.
        self.cached_num_ask = deque(maxlen=self.biggest_window)
        self.cached_num_bid = deque(maxlen=self.biggest_window)
        self.cached_num_ask.extend([np.nan] * self.biggest_window)
        self.cached_num_bid.extend([np.nan] * self.biggest_window)

        self.targeting_series_num_ask = num_ask
        self.targeting_series_num_bid = num_bid

    def on_message(self, message):
        """
        on receiving a bar message
        :param message:
        :return:
        """
        self.cached_num_ask.append(message[self.targeting_series_num_ask])
        self.cached_num_bid.append(message[self.targeting_series_num_bid])

        to_publish = {
            Tf.last_price: message[Tf.last_price],
            Tf.local_time: message[Tf.local_time],
            Tf.ask: message[Tf.ask],
            Tf.bid: message[Tf.bid],

        }

        # append ma fields for all window widths
        for width in self.window_widths:
            to_publish['sum_num_ask_window_' + str(width)] = sum(
                list(self.cached_num_ask)[(self.biggest_window - width)::])
            to_publish['sum_num_bid_window_' + str(width)] = sum(
                list(self.cached_num_bid)[(self.biggest_window - width)::])
            to_publish['ask_bid_ratio_window_' + str(width)] = sum(
                list(self.cached_num_ask)[(self.biggest_window - width)::]) - sum(
                list(self.cached_num_bid)[(self.biggest_window - width)::])

        self.publish(to_publish)


def hermes_pub(instrument):
    data_handler = HermesDataHandler()
    data_handler.add_instrument(instrument)
    data_handler.replay_data(attach_end_flag=True)

'''
3rd change
pls be careful of the name foremat in redis
there is no . in md
'''

def CountDeltaVol_sub(instrument,param_list):
    my_signal_vol_tick = CountDeltaVolBatch(
        [
            # ****'md:{}.'.format(instrument)
            'md:{}'.format(instrument)
        ],
        param_list
    )
    my_signal_vol_tick.start()

def CountDeltaVolWindow_sub(instrument,param_list):
    my_signal_vol_window = CountDeltaVolWindowBatch(
        [
            'signal:trade_vol_delta.md.'.format(instrument)
        ],
        param_list
    )
    my_signal_vol_window.start()

#def sub_count_vol_on_tick(instrument):
#    signal_count_vol_on_tick = CountVolOnTickBatch(
#        [
#            'md:{}'.format(instrument)
#        ]
#    )
#    signal_count_vol_on_tick.start()

#def sub_count_vol_on_window(instrument,param_list):
#    signal_count_vol_on_window = CountVolOnWindowBatch(
#        [
#            'md:{}'.format(instrument)
#        ],
#        param_list
#    )
#    signal_count_vol_on_window.start()



def show_signal():
    win = pg.GraphicsWindow()

    win.setWindowTitle('pyqtgraph example: Scrolling Plots')

    p1 = win.addPlot()
    data1 = deque(maxlen=60)
    curve1 = p1.plot(data1)

def window(instrument):
    athena = AthenaMainWindowController()
    # athena.add_cta2_instance(instrument, [110, 60, 40, 2.5, 0.25])
    athena.add_signal_instance(instrument, )
    QtGui.QApplication.instance().exec_()


if __name__ == '__main__':
    mp.set_start_method('spawn')

    inst = 'GC1608'

    #p_ui = mp.Process(target=window, args=(inst,))

    # p_ui = mp.Process(target=show_signal)

    processes = []
    p1 = mp.Process(target=CountDeltaVol_sub, args=(inst,[['last_price', 'volume', 'bid_vol', 'ask_vol']],))
    #p2 = mp.Process(target=CountDeltaVolWindow_sub, args=(inst,[10,20],))
    # p2 = mp.Process(target=show_signal)
    processes.extend([p1])
    #processes.extend([p2])


    p_pub =mp.Process(target=hermes_pub,args=(inst,))

    #p_ui.start()
    time.sleep(1)
    [p.start() for p in processes]

    time.sleep(1)
    p_pub.start()






















