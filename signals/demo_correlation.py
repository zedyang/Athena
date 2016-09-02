import time
import numpy as np
import multiprocessing as mp

from collections import deque
from Athena.settings import AthenaConfig
from Athena.signals.signal import SignalTemplate
from Athena.data_handler.data_handler import HermesDataHandler

Tf, Kf = AthenaConfig.HermesTickFields, AthenaConfig.HermesKLineFields

__author__ = 'Atom'


class CorrelationBatch(SignalTemplate):
    """


    """
    signal_name_prefix = 'signal:correlation'
    param_names = ['contracts', 'window_widths', 'latency_widths']

    # latency_widths must >0 for example [1,3,4,6,10]

    def __init__(self, subscribe_list, param_list, duplicate=1):
        """


        :param subscribe_list:
        :param param_list:
        :param duplicate:
        """
        super(CorrelationBatch, self).__init__(
            subscribe_list, duplicate
        )

        # self._map_to_channels(param_list,
        #                      suffix=subscribe_list[0].splite(':')[-1])
        self._map_to_channels(param_list)

        # set
        self.tag = 'correlation'
        self.contracts = self.param_dict['contracts']
        self.primary_contract = self.contracts[0]
        self.secondary_contract = self.contracts[1]
        self.window_widths = self.param_dict['window_widths']
        self.latency_widths = self.param_dict['latency_widths']
        # *2 is because the 2 side of the latency parm ---- deleted
        # *1 is because we will sepereted 2 mode dependng which is the lead
        #    and the lag contract will record the lastes data
        self.longest_widths = max(self.window_widths) + max(self.latency_widths)

        # initialize market price deque as [nan] list
        # self.cached_price = deque(maxlen=self.longest_widths)
        # self.cached_price.extend([np.nan] * self.longest_widths)

        self.cached_price = {
            self.primary_contract: np.nan,
            self.secondary_contract: np.nan
        }

        self.cached_primary_price = deque(maxlen=self.longest_widths)
        self.cached_primary_price.extend([np.nan] * self.longest_widths)
        self.cached_secondary_price = deque(maxlen=self.longest_widths)
        self.cached_secondary_price.extend([np.nan] * self.longest_widths)

    def on_message(self, message):
        """

        :param message:
        :return:
        """
        # update prices on bar
        if message['tag'] == 'kl':
            # temp price to record price
            self.cached_price[message[Kf.contract]] = float(message[Kf.close_price])
            # build secondary list
            if message[Kf.contract] == self.secondary_contract:
                self.cached_secondary_price.append(self.cached_price[self.secondary_contract])
                # print(self.cached_secondary_price, '---')
            # build primary list
            # calculate correlation based on primary
            elif message[Kf.contract] == self.primary_contract:
                self.cached_primary_price.append(self.cached_price[self.primary_contract])
                # print(self.cached_primary_price, '***')

                array_primary_price = np.array(self.cached_primary_price)
                array_secondary_price = np.array(self.cached_secondary_price)

                to_publish = {
                    Kf.open_time: message[Kf.open_time],
                    Kf.close_time: message[Kf.close_time],

                }

                for widths_window in self.window_widths:
                    temp_primary_lag = array_primary_price[-widths_window:]
                    temp_secondary_lag = array_secondary_price[-widths_window:]
                    # print('temp primary lag', temp_primary_lag)
                    # print('temp secondary lag', temp_secondary_lag)

                    to_publish[str(widths_window)+','+'0'] = np.corrcoef(temp_secondary_lag, temp_primary_lag)[0][1]
                    # print('correlation', np.corrcoef(temp_secondary_lag,temp_primary_lag)[0][1])
                    for widths_latency in self.latency_widths:
                        temp_primary_lead = array_primary_price[-widths_window-widths_latency:-widths_latency]
                        temp_secondary_lead = array_secondary_price[-widths_window-widths_latency:-widths_latency]
                        print(temp_primary_lead)

                        # if primary lead, the widths_latency is +
                        to_publish[str(widths_window) + ',' + str(widths_latency)] = \
                            np.corrcoef(temp_primary_lead, temp_secondary_lag)[0][1]

                        # if secondary lead, the widths_latency is -
                        to_publish[str(widths_window) + ',' + str(-widths_latency)] = \
                            np.corrcoef(temp_secondary_lead, temp_primary_lag)[0][1]

                self.publish(to_publish)


def sub_correlation():
    signal_correlation = CorrelationBatch(
        [
            'kl:{}.3s'.format('GC1612'),
            'kl:{}.3s'.format('Au(T+D)')
        ],
        param_list=[('GC1612', 'Au(T+D)'),
                    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                     16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
                     30],
                    [1, 2, 3, 4, 5, 6, 7, 8]]
    )
    signal_correlation.start()


def hermes_pub():
    data_handler = HermesDataHandler()
    data_handler.add_instrument(instrument='GC1612', kline_dur_specifiers=('3s',))
    data_handler.add_instrument(instrument='Au(T+D)', kline_dur_specifiers=('3s',))
    # data_handler.replay_data(attach_end_flag=True)
    data_handler.distribute_data()


if __name__ == '__main__':
    mp.set_start_method('spawn')

    processes = []
    p1 = mp.Process(target=sub_correlation)
    # p2 = mp.Process(target=show_signal)
    processes.extend([p1])

    p_pub = mp.Process(target=hermes_pub)

    # p_ui.start()
    time.sleep(1)
    [p.start() for p in processes]

    time.sleep(1)
    p_pub.start()





