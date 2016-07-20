import json

from datetime import datetime, timedelta
from operator import add

from Athena.settings import AthenaConfig
from Athena.utils import append_digits_suffix_for_redis_key
from Athena.signals.signal import SingleInstrumentSignal

__author__ = 'zed'


class GeneralBar(SingleInstrumentSignal):
    """
    Implementation of generalized bar of any time period.
    """

    def __init__(self, instrument, period=timedelta(minutes=1), tag=None):
        """
        Constructor.
        :param period: the sampling time interval of the bar.
        :param instrument:
        """
        # set signal name.
        super(GeneralBar, self).__init__(instrument, tag)
        self.time_interval = period
        self.two_time_intervals = period + period
        self.time_interval_as_secs = period.seconds
        self.time_interval_as_minutes = period.seconds // 60

        # reset signal name and pub channel
        if self.time_interval_as_minutes > 0:
            self.signal_name = 'signal:bar' + '_' + \
                               str(self.time_interval_as_minutes) + 'm'
        else:
            self.signal_name = 'signal:bar' + '_' + \
                               str(self.time_interval_as_secs) + 's'
        self.pub_channel = self.signal_name + '_' + self.instrument

        # initialize the bar buffers
        self.__reset_bar()
        # record the start point of the bar.
        self.last_open_time = None

        # record the close price of last minute.
        self.last_close_price = {
            'ask': 0,
            'bid': 0
        }

        # also maintains a data repo for plotting
        self.plot_data_channel = 'plot:' + self.pub_channel

    def __reset_bar(self):
        """
        reset bar to empty
        :return:
        """
        self.cached_prices = {
            'ask': [],  # ask
            'bid': [],
            'mid_point': []
        }
        self.cached_volumes = {
            'ask': 0,
            'bid': 0,
            'new': 0
        }

    def publish_plot_data(self, new_bar):
        """

        :return:
        """
        published_key = append_digits_suffix_for_redis_key(
            prefix=self.plot_data_channel,
            counter=self.counter
        )
        plot_data = {
            'end_time': new_bar['end_time'],
            'open': new_bar['open'],
            'high': new_bar['high'],
            'low': new_bar['low'],
            'close': new_bar['close'],
            'count': new_bar['count']
        }

        # publish_md message
        self.signal_wrapper.set_dict(published_key, plot_data)

        plot_data_str = json.dumps(plot_data)
        self.signal_wrapper.connection.publish(
            channel=self.plot_data_channel,
            message=plot_data_str
        )

    def on_message(self, message):
        """

        :param message:
        :return:
        """

        str_time = \
            message[AthenaConfig.sql_local_dt_field]
        curr_time = datetime.strptime(str_time,
                                      AthenaConfig.dt_format)
        # initialization.
        if not self.last_open_time:
            self.last_open_time = curr_time

        # compress ticks into bar
        if curr_time - self.last_open_time <= self.time_interval:
            self.cached_prices['bid'].append(
                float(message[AthenaConfig.sql_bid_field])
            )
            self.cached_prices['ask'].append(
                float(message[AthenaConfig.sql_ask_field])
            )
            self.cached_volumes['bid'] += float(
                message[AthenaConfig.sql_bid_vol_field]
            )
            self.cached_volumes['ask'] += float(
                message[AthenaConfig.sql_ask_vol_field]
            )
        else:
            # finish this bar

            # set the bar data
            # * if there is no bid in this minute,
            # use bid = CLOSE of last minute, and bid volume = 0
            if not self.cached_prices['ask']:
                self.cached_prices['ask'].append(self.last_close_price['ask'])
            if not self.cached_prices['bid']:    # same for bid.
                self.cached_prices['bid'].append(self.last_close_price['bid'])

            bar_data = {
                'contract': self.instrument,
                'start_time': self.last_open_time.strftime(
                    AthenaConfig.dt_format
                ),
                'end_time': (self.last_open_time +
                             self.time_interval).strftime(
                    AthenaConfig.dt_format
                ),
                'ask_open': self.cached_prices['ask'][0],
                'ask_high': max(self.cached_prices['ask']),
                'ask_low': min(self.cached_prices['ask']),
                'ask_close': self.cached_prices['ask'][-1],
                'bid_open': self.cached_prices['bid'][0],
                'bid_high': max(self.cached_prices['bid']),
                'bid_low': min(self.cached_prices['bid']),
                'bid_close': self.cached_prices['bid'][-1],
                'ask_volume': self.cached_volumes['bid'],
                'bid_volume': self.cached_volumes['bid'],
                'open': (
                    (self.cached_prices['ask'][0] +
                     self.cached_prices['bid'][0]) / 2
                ),
                'high': max(map(add, self.cached_prices['ask'],
                                self.cached_prices['bid'])) / 2,
                'low': min(map(add, self.cached_prices['ask'],
                               self.cached_prices['bid'])) / 2,
                'close': (
                    (self.cached_prices['ask'][-1] +
                     self.cached_prices['bid'][-1]) / 2
                ),
                'count': self.counter
            }

            # publish_md the message
            self.publish(bar_data)
            self.publish_plot_data(bar_data)

            # reset timer
            if curr_time - self.last_open_time > self.two_time_intervals:
                # one or more minute in which there is no tick
                self.last_open_time = curr_time
            else:
                # curr_time is in next minute
                self.last_open_time = self.last_open_time + self.time_interval

            # reset last_close_price
            self.last_close_price['ask'] = bar_data['ask_close']
            self.last_close_price['bid'] = bar_data['bid_close']

            # clear up the current bar
            self.__reset_bar()
            # append this tick that initiate next bar to the list.
            self.cached_prices['bid'].append(
                float(message[AthenaConfig.sql_bid_field])
            )
            self.cached_prices['ask'].append(
                float(message[AthenaConfig.sql_ask_field])
            )
            self.cached_volumes['bid'] += float(
                message[AthenaConfig.sql_bid_vol_field]
            )
            self.cached_volumes['ask'] += float(
                message[AthenaConfig.sql_ask_vol_field]
            )

            print('----')
            print(bar_data)
            # counter +=1 only when finish a bar.
            self.counter += 1

