from collections import deque

from Athena.settings import AthenaConfig
from Athena.utils import append_digits_suffix_for_redis_key
from Athena.data_handler.redis_wrapper import RedisWrapper

__author__ = 'zed'


class HermesDataSorter(object):
    """
    A small module that sorts hermes md/kline data according to time
    sequence and then interlace them together into one deck.
    """

    ft_delta_second = 10000000

    def __init__(self, instrument, kl_duration):
        """
        constructor
        :param instrument: string
        :param kl_duration: string
        :return:
        """
        # open two connections
        self.sub_wrapper = RedisWrapper(
            db=AthenaConfig.hermes_db_index
        )
        self.pub_wrapper = RedisWrapper(
            db=AthenaConfig.historical_md_db_index
        )
        self.kline_duration_specifier = kl_duration
        self.instrument = instrument

        # channels
        self.md_channel = AthenaConfig.hermes_nanhua_md_dir + instrument
        self.kline_channel = AthenaConfig.hermes_nanhua_kl_dir + instrument \
                             + '.' + self.kline_duration_specifier

        # sorted deck
        self.sorted_keys = None

    @staticmethod
    def timestamp(key):
        return int(key.decode('utf-8').split(':')[-1])

    def __sort_deck(self):
        """

        :return:
        """
        md_hist_keys = deque(self.sub_wrapper.get_keys(
            '{}:*'.format(self.md_channel)
        ))
        kl_hist_keys = deque(self.sub_wrapper.get_keys(
            '{}:*'.format(self.kline_channel)
        ))

        deck = []
        # iterate and sort the deck
        for k in md_hist_keys:
            try:
                md_time = HermesDataSorter.timestamp(k)
                next_kline_start = HermesDataSorter.timestamp(kl_hist_keys[0])
                next_kline_end = next_kline_start \
                                 + 60 * HermesDataSorter.ft_delta_second
                # if tick exceed current kline
                if md_time > next_kline_end:
                    kk = kl_hist_keys.popleft()
                    deck.append(kk)

                deck.append(k)
            except UnicodeError:
                print('[Data Sorter]: Unicode error at key {}.'.format(k))
                continue

        self.sorted_keys = deck

    def prepare_temporal_stream(self):
        """

        :return:
        """
        # flush db (historical data)
        self.pub_wrapper.flush_db()

        if not self.sorted_keys:
            self.__sort_deck()

        counter = 0
        for k in self.sorted_keys:

            # get dict data and set new key
            try:
                dict_data = self.sub_wrapper.get_dict(k)
            except UnicodeError:
                print('[Data Handler]: Unicode error at key {}.'.format(k))
                continue

            new_key_in_redis = append_digits_suffix_for_redis_key(
                prefix=AthenaConfig.redis_temp_hist_stream_dir,
                counter=counter
            )
            # append contract field
            dict_data[AthenaConfig.sql_instrument_field] = self.instrument
            # set data in redis
            self.pub_wrapper.set_dict(
                key=new_key_in_redis,
                data=dict_data
            )
            # increment to counter
            counter += 1

if __name__ == '__main__':
    proxy = HermesDataSorter('GC1608', '1m')
    proxy.prepare_temporal_stream()
