import json
import time

from Athena.data_handler.redis_wrapper import RedisWrapper
from Athena.settings import AthenaConfig, AthenaProperNames
from Athena.utils import append_digits_suffix_for_redis_key

__author__ = 'zed'


class BacktestDriver(object):
    """

    """

    def __init__(self, instruments_list):
        """

        :param instruments_list: list of strings
        """
        self.instruments_list = instruments_list

        # open two connections to redis
        self.sub_wrapper = RedisWrapper(db=AthenaConfig.historical_md_db_index)
        self.pub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)

        # initialize a dictionary of counters_md
        self.counters = dict()
        for instrument in instruments_list:
            self.counters[instrument] = 0

        # keys to iterate through.
        self.hist_keys = self.sub_wrapper.get_keys(
            '{}:*'.format(AthenaConfig.redis_md_dir))

    def __stream_record(self, key):
        """
        pop a record by key from the backtest data repo and publish it to
        corresponding channel by instruments.
        :param key: string, the redis key of record.
        :return:
        """
        hist_data = self.sub_wrapper.get_dict(key)

        # end flag encountered
        if AthenaConfig.redis_md_end_flag in hist_data:
            print('[MD Handler]: Done, historical data is exhausted.')
            for instrument in self.instruments_list:
                eof_key = 'md:' + instrument + ':' +\
                          AthenaConfig.redis_key_max_digits * '9'
                self.sub_wrapper.set_dict(eof_key, hist_data)
            return 0
        # otherwise

        # inspect instrument name
        this_instrument = hist_data[AthenaConfig.sql_instrument_field]

        # set key in redis
        new_key_in_redis = append_digits_suffix_for_redis_key(
            prefix='md:' + this_instrument,
            counter=self.counters[this_instrument]
        )

        # publish dict data
        hist_data['type'] = AthenaProperNames.md_message_type
        self.pub_wrapper.set_dict(new_key_in_redis, hist_data)
        message = json.dumps({new_key_in_redis: hist_data})
        self.pub_wrapper.connection.publish(
            channel='md:' + this_instrument,
            message=message
        )

        # increment to counter
        self.counters[this_instrument] += 1
        return 1

    def distribute_data(self, stream_interval=None):
        """
        begin to distribute historical data.
        :param stream_interval:
        :return:
        """
        # flush athena_db (1)
        self.pub_wrapper.flush_db()

        for hist_key in self.hist_keys:
            flag = self.__stream_record(hist_key)
            if flag == 0:
                return
            if stream_interval:
                time.sleep(stream_interval)

