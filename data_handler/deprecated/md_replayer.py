import json

from Athena.settings import AthenaConfig, AthenaProperNames
from Athena.utils import append_digits_suffix_for_redis_key
from Athena.data_handler.redis_wrapper import RedisWrapper
from Athena.data_handler.real_time_handler import HermesDataHandler, \
    clean_hermes_data

__author__ = 'zed'


class HermesDataReplayHandler(object):
    """

    """
    def __init__(self, instruments_list, duration):
        """

        :return:
        """
        self.instruments_list = instruments_list
        self.kline_duration_specifier = duration

        # the counters for md(tick) and k-lines.
        self.counters_md = dict()
        for inst in self.instruments_list:
            self.counters_md[inst] = 0

        self.counters_kline = dict()
        for inst in self.instruments_list:
            self.counters_kline[inst] = 0

        # open connections
        self.sub_wrapper = RedisWrapper(
            db=AthenaConfig.historical_md_db_index
        )
        self.pub_wrapper = RedisWrapper(
            db=AthenaConfig.athena_db_index
        )

        self.replay_channel = AthenaConfig.redis_temp_hist_stream_dir

        # set pub channels
        self.pub_channels_kline = dict()
        for inst in self.instruments_list:
            self.pub_channels_kline[inst] = 'signal:' + 'kl.' + inst + '.' \
            + self.kline_duration_specifier

        self.pub_channels_kline_plots = dict()
        for inst in self.instruments_list:
            self.pub_channels_kline_plots[inst] = 'plot:signal:' + 'kl.' \
            + inst + '.' + self.kline_duration_specifier

    def publish_md(self, dict_data):
        """
        publish a tick dictionary data to redis pub channel.
        :param dict_data: dict
        :return:
        """
        # map to new key
        this_instrument = dict_data[AthenaConfig.sql_instrument_field]
        new_key_in_redis = append_digits_suffix_for_redis_key(
            prefix='md:' + this_instrument,
            counter=self.counters_md[this_instrument]
        )

        # publish dict data
        dict_data['type'] = AthenaProperNames.md_message_type
        dict_data['tag'] = 'tick'
        self.pub_wrapper.set_dict(new_key_in_redis, dict_data)

        # publish to pub channel
        # serialize datetime fields.
        dict_data[AthenaConfig.sql_exchange_dt_field] = \
            dict_data[AthenaConfig.sql_exchange_dt_field].strftime(
                AthenaConfig.dt_format)
        dict_data[AthenaConfig.sql_local_dt_field] = \
            dict_data[AthenaConfig.sql_local_dt_field].strftime(
                AthenaConfig.dt_format)

        message = json.dumps({new_key_in_redis: dict_data})
        self.pub_wrapper.connection.publish(
            channel='md:' + this_instrument,
            message=message
        )

        # increment to counter
        self.counters_md[this_instrument] += 1
        return 1

    def publish_kline(self, dict_data):
        """
        publish a tick dictionary data to redis pub channel.
        :param dict_data: dict
        :return:
        """
        # map to new key
        this_instrument = dict_data[AthenaConfig.sql_instrument_field]
        pub_channel = self.pub_channels_kline[this_instrument]
        new_key_in_redis = append_digits_suffix_for_redis_key(
            prefix=pub_channel,
            counter=self.counters_kline[this_instrument]
        )

        # update type and counter fields.
        dict_data['type'] = AthenaProperNames.signal_message_type
        dict_data['count'] = self.counters_kline[this_instrument]

        # publish dict data
        self.pub_wrapper.set_dict(new_key_in_redis, dict_data)

        # publish to pub channel
        # serialize datetime fields.
        dict_data[AthenaConfig.sql_kline_open_time_field] = \
            dict_data[
                AthenaConfig.sql_kline_open_time_field].strftime(
                AthenaConfig.dt_format)
        dict_data[AthenaConfig.sql_kline_exchange_open_time_field] = \
            dict_data[
                AthenaConfig.sql_kline_exchange_open_time_field].strftime(
                AthenaConfig.dt_format)

        message = json.dumps({new_key_in_redis: dict_data})
        self.pub_wrapper.connection.publish(
            channel=pub_channel,
            message=message
        )

        # ------------------------------------------------------------
        # publish data for plotting
        pub_channel_plot = self.pub_channels_kline_plots[this_instrument]
        new_key_in_redis = append_digits_suffix_for_redis_key(
            prefix=pub_channel_plot,
            counter=self.counters_kline[this_instrument]
        )

        plot_data = {
            'open_time': dict_data['open_time'],
            'open': dict_data['open'],
            'high': dict_data['high'],
            'low': dict_data['low'],
            'close': dict_data['close'],
            'count': dict_data['count']
        }

        # publish dict data
        self.pub_wrapper.set_dict(new_key_in_redis, plot_data)

        # publish to pub channel
        plot_message = json.dumps({new_key_in_redis: plot_data})
        self.pub_wrapper.connection.publish(
            channel=pub_channel_plot,
            message=plot_message
        )

        # increment to counter
        self.counters_kline[this_instrument] += 1
        return 1

    def replay(self):
        """

        :return:
        """
        self.pub_wrapper.flush_db()
        keys_to_replay = self.sub_wrapper.get_keys(
            '{}:*'.format(self.replay_channel)
        )

        for k in keys_to_replay:
            # loop and clean data.
            raw_data = self.sub_wrapper.get_dict(k)
            cleaned_data = clean_hermes_data(raw_data, is_hash_set=True)
            if 'bid_vol' in cleaned_data:
                self.publish_md(cleaned_data)
            elif 'open' in cleaned_data:
                self.publish_kline(cleaned_data)

if __name__ == '__main__':
    replay_proxy = HermesDataReplayHandler(['GC1608'], '1m')
    replay_proxy.replay()