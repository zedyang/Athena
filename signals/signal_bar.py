from datetime import datetime, timedelta

from Athena.settings import AthenaConfig
from Athena.signals.signal import SingleInstrumentSignal

__author__ = 'zed'


class OneMinuteBar(SingleInstrumentSignal):
    """
    Implementation of 1 minute bar.
    """

    def __init__(self, instrument):
        """
        Constructor.
        :param instrument:
        """
        super(OneMinuteBar, self).__init__(instrument)
        self.__reset_bar()
        # record the start point of the bar.
        self.last_open_time = None

    def __reset_bar(self):
        """
        reset bar to empty
        :return:
        """
        self.cached_price = {
            'ASK': [],  # ask
            'BID': []
        }
        self.cached_volumes = {
            'ASK': 0,
            'BID': 0
        }

    def on_market_data(self, data):
        """

        :param data:
        :return:
        """
        print(data)
        str_time = \
            data[AthenaConfig.ATHENA_SQL_TABLE_FIELD_DATETIME]
        curr_time = datetime.strptime(str_time,
                                      AthenaConfig.ATHENA_SQL_DT_FORMAT)
        # initialization.
        if not self.last_open_time:
            self.last_open_time = curr_time

        # compress ticks into bar
        if curr_time - self.last_open_time <= timedelta(minutes=1):
            self.cached_price[
                data[AthenaConfig.ATHENA_SQL_TABLE_FIELD_SUBTYPE]].append(
                float(data[AthenaConfig.ATHENA_SQL_TABLE_FIELD_PRICE])
            )
            self.cached_volumes[
                data[AthenaConfig.ATHENA_SQL_TABLE_FIELD_SUBTYPE]] \
                += float(data[AthenaConfig.ATHENA_SQL_TABLE_FIELD_VOLUME])
        else:
            # finish this bar
            # set the key
            distribute_key = self.instrument + '_bar_1m_signal:' \
                             + (self.digits - len(str(self.counter))) * '0' \
                             + str(self.counter)
            # set data
            data = {
                'START_TIME': datetime.strftime(
                    self.last_open_time, AthenaConfig.ATHENA_SQL_DT_FORMAT),
                'END_TIME': datetime.strftime(
                    self.last_open_time + timedelta(minutes=1),
                    AthenaConfig.ATHENA_SQL_DT_FORMAT),
                'ASK_OPEN': self.cached_price['ASK'][0],
                'ASK_HIGH': max(self.cached_price['ASK']),
                'ASK_LOW': min(self.cached_price['ASK']),
                'ASK_CLOSE': self.cached_price['ASK'][-1],
                'BID_OPEN': self.cached_price['BID'][0],
                'BID_HIGH': max(self.cached_price['BID']),
                'BID_LOW': min(self.cached_price['BID']),
                'BID_CLOSE': self.cached_price['BID'][-1],
                'ASK_VOLUME': self.cached_volumes['ASK'],
                'BID_VOLUME': self.cached_volumes['BID'],
            }

            self.signal_api.set_dict(distribute_key, data)
            # clean up current bar
            self.last_open_time = self.last_open_time + timedelta(minutes=1)
            self.__reset_bar()
            # counter +=1 only when finish a bar.
            self.counter += 1