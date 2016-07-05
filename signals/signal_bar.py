from datetime import datetime, timedelta

from Athena.settings import AthenaConfig
from Athena.signals.signal import SingleInstrumentSignal

__author__ = 'zed'


class GeneralBar(SingleInstrumentSignal):
    """
    Implementation of generalized bar of any time period.
    """

    def __init__(self, instrument, period=timedelta(minutes=1)):
        """
        Constructor.
        :param period: the sampling time interval of the bar.
        :param instrument:
        """
        # set signal name.
        super(GeneralBar, self).__init__(instrument)
        self.time_interval = period
        self.two_time_intervals = period + period
        self.time_interval_as_secs = period.seconds
        self.time_interval_as_minutes = period.seconds // 60

        # reset signal name and pub channel
        self.signal_name = 'bar' + '_' + \
                           str(self.time_interval_as_minutes) + 'm'
        self.pub_channel = self.signal_name + '_' + self.instrument

        # initialize the bar buffers
        self.__reset_bar()
        # record the start point of the bar.
        self.last_open_time = None

        # record the close price of last minute.
        self.last_close_price = {
            'ASK': 0,
            'BID': 0
        }

    def __reset_bar(self):
        """
        reset bar to empty
        :return:
        """
        self.cached_prices = {
            'ASK': [],  # ask
            'BID': [],
            'NEW': []
        }
        self.cached_volumes = {
            'ASK': 0,
            'BID': 0,
            'NEW': 0
        }

    def on_message(self, message):
        """

        :param message:
        :return:
        """

        str_time = \
            message[AthenaConfig.ATHENA_SQL_TABLE_FIELD_DATETIME]
        curr_time = datetime.strptime(str_time,
                                      AthenaConfig.ATHENA_SQL_DT_FORMAT)
        # initialization.
        if not self.last_open_time:
            self.last_open_time = curr_time

        # compress ticks into bar
        if curr_time - self.last_open_time <= self.time_interval:
            self.cached_prices[
                message[AthenaConfig.ATHENA_SQL_TABLE_FIELD_SUBTYPE]].append(
                float(message[AthenaConfig.ATHENA_SQL_TABLE_FIELD_PRICE])
            )
            self.cached_volumes[
                message[AthenaConfig.ATHENA_SQL_TABLE_FIELD_SUBTYPE]] \
                += float(message[AthenaConfig.ATHENA_SQL_TABLE_FIELD_VOLUME])
        else:
            print('----')
            print(self.cached_prices)
            print(self.cached_volumes)
            # finish this bar

            # set the bar data
            # * if there is no bid in this minute,
            # use bid = CLOSE of last minute, and bid volume = 0
            if not self.cached_prices['ASK']:
                self.cached_prices['ASK'].append(self.last_close_price['ASK'])
            if not self.cached_prices['BID']:    # same for bid.
                self.cached_prices['BID'].append(self.last_close_price['BID'])

            bar_data = {
                'START_TIME': self.last_open_time.strftime(
                    AthenaConfig.ATHENA_SQL_DT_FORMAT
                ),
                'END_TIME': (self.last_open_time +
                             self.time_interval).strftime(
                    AthenaConfig.ATHENA_SQL_DT_FORMAT
                ),
                'ASK_OPEN': self.cached_prices['ASK'][0],
                'ASK_HIGH': max(self.cached_prices['ASK']),
                'ASK_LOW': min(self.cached_prices['ASK']),
                'ASK_CLOSE': self.cached_prices['ASK'][-1],
                'BID_OPEN': self.cached_prices['BID'][0],
                'BID_HIGH': max(self.cached_prices['BID']),
                'BID_LOW': min(self.cached_prices['BID']),
                'BID_CLOSE': self.cached_prices['BID'][-1],
                'ASK_VOLUME': self.cached_volumes['ASK'],
                'BID_VOLUME': self.cached_volumes['BID'],
            }

            # publish the message
            self.publish_message(bar_data)

            # reset timer
            if curr_time - self.last_open_time > self.two_time_intervals:
                # one or more minute in which there is no tick
                self.last_open_time = curr_time
            else:
                # curr_time is in next minute
                self.last_open_time = self.last_open_time + self.time_interval

            # reset last_close_price
            self.last_close_price['ASK'] = bar_data['ASK_CLOSE']
            self.last_close_price['BID'] = bar_data['BID_CLOSE']

            # clear up the current bar
            self.__reset_bar()
            # append this tick that initiate next bar to the list.
            self.cached_prices[
                message[AthenaConfig.ATHENA_SQL_TABLE_FIELD_SUBTYPE]].append(
                float(message[AthenaConfig.ATHENA_SQL_TABLE_FIELD_PRICE])
            )
            self.cached_volumes[
                message[AthenaConfig.ATHENA_SQL_TABLE_FIELD_SUBTYPE]] \
                += float(message[AthenaConfig.ATHENA_SQL_TABLE_FIELD_VOLUME])

            # counter +=1 only when finish a bar.
            self.counter += 1

