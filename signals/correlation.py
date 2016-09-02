import time
import numpy as np
import multiprocessing as mp

from collections import deque
from Athena.settings import AthenaConfig
from Athena.signals.signal import SignalTemplate
from Athena.data_handler.data_handler import HermesDataHandler

Tf, Kf = AthenaConfig.TickFields, AthenaConfig.KLineFields

__author__ = 'Atom'


class CorrelationBatch(SignalTemplate):
    """


    """
    signal_name_prefix = 'signal:correlation'
    param_names = ['target_A', 'tag_A', 'key_A',
                   'target_B', 'tag_B', 'key_B',
                   'window_widths', 'latency_widths']

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
        self.target_A = self.param_dict['target_A']
        self.tag_A = self.param_dict['tag_A']
        self.key_A = self.param_dict['key_A']
        self.target_B = self.param_dict['target_B']
        self.tag_B = self.param_dict['tag_B']
        self.key_B = self.param_dict['key_B']
        self.window_widths = self.param_dict['window_widths']
        self.latency_widths = self.param_dict['latency_widths']

        # *2 is because the 2 side of the latency parm ---- deleted
        # *1 is because we will sepereted 2 mode dependng which is the lead
        #    and the lag contract will record the lastes data
        self.longest_widths = max(self.window_widths) + max(self.latency_widths)

        self.cached_price = {
            self.target_A_price: np.nan,
            self.target_B_price: np.nan
        }

        self.cached_target_A_price = deque(maxlen=self.longest_widths)
        self.cached_target_A_price.extend([np.nan] * self.longest_widths)
        self.cached_target_B_price = deque(maxlen=self.longest_widths)
        self.cached_target_B_price.extend([np.nan] * self.longest_widths)

    def on_message(self, message):
        """

        :param message:
        :return:
        """
        # update prices on bar

        if message['tag'] == self.tag_B:
            self.cached_target_B_price.append(float(message[self.key_B]))

        if message['tag'] == self.tag_A:
            self.cached_target_A_price.append(float(message[self.key_A]))

            array_target_A = np.array(self.cached_target_A_price)
            array_target_B = np.array(self.cached_target_B_price)

            to_publish = {
                'open_time': message['open_time'],
                'end_time': message['end_time']
            }

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
                    Kf.end_time: message[Kf.end_time],

                }

                for widths_window in self.window_widths:
                    temp_primary_lag = array_primary_price[-widths_window:]
                    temp_secondary_lag = array_secondary_price[-widths_window:]
                    # print('temp primary lag', temp_primary_lag)
                    # print('temp secondary lag', temp_secondary_lag)

                    to_publish[str(widths_window)+','+'0'] = \
                        np.corrcoef(temp_secondary_lag, temp_primary_lag)[0][1]

                    for widths_latency in self.latency_widths:
                        temp_primary_lead = \
                            array_primary_price[(
                                -widths_window-widths_latency):-widths_latency]
                        temp_secondary_lead = \
                            array_secondary_price[(
                                -widths_window-widths_latency):-widths_latency]
                        print(temp_primary_lead)

                        # if primary lead, the widths_latency is +
                        to_publish[str(widths_window) + ','
                                   + str(widths_latency)] = \
                            np.corrcoef(
                                temp_primary_lead, temp_secondary_lag)[0][1]

                        # if secondary lead, the widths_latency is -
                        to_publish[str(widths_window) + ',' + str(-widths_latency)] = \
                            np.corrcoef(temp_secondary_lead, temp_primary_lag)[0][1]

                self.publish(to_publish)


def sub_correlation():
    signal_correlation = CorrelationBatch(
        [
            'kl:{}.1m'.format('GC1612'),
            'kl:{}.1m'.format('Au(T+D)')
        ],
        param_list=[('GC1612', 'Au(T+D)'), [4, 5], [1, 2]]
    )
    signal_correlation.start()


def hermes_pub():
    data_handler = HermesDataHandler()
    data_handler.add_instrument('GC1612')
    data_handler.add_instrument('Au(T+D)')
    data_handler.replay_data(attach_end_flag=True)


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





