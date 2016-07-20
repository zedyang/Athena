import json
import time
from datetime import datetime

from Athena.settings import AthenaConfig, AthenaProperNames
from Athena.utils import filetime_to_dt, dt_to_filetime, \
    append_digits_suffix_for_redis_key
from Athena.data_handler.redis_wrapper import RedisWrapper

__author__ = 'zed'


class DataHandler(object):
    """

    """
    pass


class HermesDataHandler(DataHandler):
    """

    """

    replay_time_interval = 1

    def __init__(self, instruments_list):
        """

        :param instruments_list: list of strings
        :return:
        """
        self.instruments_list = instruments_list

        # the flag marking whether the history before the init of Athena
        # is recovered.
        self.md_history_fixed = dict()
        for inst in self.instruments_list:
            self.md_history_fixed[inst] = False

        self.kline_history_fixed = dict()
        for inst in self.instruments_list:
            self.kline_history_fixed[inst] = False

        # the counters for md(tick) and klines.
        self.counters_md = dict()
        for inst in self.instruments_list:
            self.counters_md[inst] = 0

        self.counters_kline = dict()
        for inst in self.instruments_list:
            self.counters_kline[inst] = 0

        # map to subscribe channels.
        self.sub_channels_tick = [AthenaConfig.hermes_nanhua_md_dir + inst
                                  for inst in self.instruments_list]
        self.kline_duration_specifier = '1m'
        self.sub_channels_kline = [
            AthenaConfig.hermes_nanhua_kl_dir + inst + '.'
            + self.kline_duration_specifier
            for inst in self.instruments_list
        ]
        self.__subscribe()

        # set pub channels
        self.pub_channels_kline = dict()
        for inst in self.instruments_list:
            self.pub_channels_kline[inst] = 'signal:' + 'kl.' + inst + '.' \
            + self.kline_duration_specifier

        self.pub_channels_kline_plots = dict()
        for inst in self.instruments_list:
            self.pub_channels_kline_plots[inst] = 'plot:signal:' + 'kl.' \
            + inst + '.' + self.kline_duration_specifier

    def __subscribe(self):
        """
        open connection and subscribe channels.
        :return:
        """
        # open two connections
        self.sub_wrapper = RedisWrapper(db=AthenaConfig.hermes_db_index)
        self.pub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)

        # create a subscriber.
        self.sub = self.sub_wrapper.connection.pubsub()

        # subscribe
        self.sub.subscribe(self.sub_channels_tick)
        self.sub.subscribe(self.sub_channels_kline)

        # clean up existing keys in athena md repo (not Hermes md repo!).
        for inst in self.instruments_list:
            if not self.md_history_fixed[inst]:
                self.pub_wrapper.flush_db()

    @staticmethod
    def __clean_data(message, is_hash_set=False):
        """
        clean tick data message.
        :param message: message received from sub channel.
        :param is_hash_set: bool, specifies whether the message is a dict
        from sub.listen() or that from RedisWrapper.get_dict()
            - True: from RedisWrapper.get_dict()
            - False: from sub.listen()
        Default is False.
        :return:
        """
        # convert types
        if not is_hash_set:
            str_message = message['data'].decode('utf-8')
            list_message = str_message.split(AthenaConfig.hermes_md_sep_char)
            dict_message = dict(zip(list_message[0::2], list_message[1::2]))
        else:
            dict_message = message
        try:
            if 'bid1' in dict_message:     # if tick (md) message

                # clean tick data
                ex_update_time = filetime_to_dt(
                    int(dict_message['systime']))
                local_update_time = filetime_to_dt(
                    int(dict_message['subtime']))

                if not is_hash_set:
                    # contract field is settled here
                    # only when from sub.listen()
                    contract = dict_message['key'].split(':')[0].split('.')[-1]
                else:
                    contract = dict_message[AthenaConfig.sql_instrument_field]

                bid = float(dict_message['bid1']) / 10000
                ask = float(dict_message['ask1']) / 10000
                last_price = float(dict_message['lastprice']) / 10000

                # make dict_data
                dict_data = {
                    'trade_day': dict_message['day'],
                    'ex_update_time': ex_update_time,
                    'local_update_time': local_update_time,
                    'contract': contract,
                    'ask': ask,
                    'bid': bid,
                    'ask_vol': dict_message['askvol1'],
                    'bid_vol': dict_message['bidvol1'],
                    'volume': dict_message['volume'],
                    'open_interest': dict_message['openinterest'],
                    'last_price': last_price,
                    'tag': 'tick'
                }

            elif 'openwndtime' in dict_message:      # kline message

                # clean kline data
                ex_update_time = filetime_to_dt(
                    int(dict_message['opensystime']))
                local_update_time = filetime_to_dt(
                    int(dict_message['openwndtime']))

                if not is_hash_set:
                    # contract field is settled here
                    # only when from sub.listen()
                    contract = dict_message['key'].split(':')[0].split('.')[-2]
                else:
                    contract = dict_message[AthenaConfig.sql_instrument_field]

                open_price = float(dict_message['open']) / 10000
                high_price = float(dict_message['high']) / 10000
                low_price = float(dict_message['low']) / 10000
                close_price = float(dict_message['close']) / 10000

                # make dict_data
                dict_data = {
                    'ex_open_time': ex_update_time,
                    'open_time': local_update_time,
                    'contract': contract,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price,
                    'volume': dict_message['volume'],
                    'open_interest': dict_message['openinterest'],
                    'duration': dict_message['dur'],
                    'tag': 'kline'
                }

            else:
                raise KeyError
        except KeyError:
            # when there is some fields missing
            print('[Data Handler]: Incomplete message skipped.')
            return dict()
        return dict_data

    def __fix_history(self, merge_point, instrument,
                      is_kline=False, coerced_replay=False):
        """
        Recover history data of an instrument from hermes repo
        :param merge_point: string, the point to merge existing history and
        recently pushed data from Hermes.
        The merge point is currrently implemented as the local_update_time
        :param instrument: string
        :param is_kline: boolean,  fix kline or md history
        :param coerced_replay: boolean, in replay mode (replay all data in
            db0) or not.
        :return:
        """
        if merge_point:
            merge_point_ft = dt_to_filetime(merge_point)
        else:
            merge_point_ft = -1

        if not is_kline:
            channel = AthenaConfig.hermes_nanhua_md_dir + instrument
            dt_field = AthenaConfig.hermes_local_dt_field
        else:
            channel = AthenaConfig.hermes_nanhua_kl_dir + instrument \
                      + '.' + self.kline_duration_specifier
            dt_field = AthenaConfig.hermes_kline_open_time_field

        hist_keys = self.sub_wrapper.get_keys('{}:*'.format(channel))

        # retrieve historical data
        for k in hist_keys:
            try:
                row = self.sub_wrapper.get_dict(k)
            except UnicodeError:
                print('[Data Handler]: Unicode error at key {}.'.format(k))
                continue

            # instrument field should be resolved here
            row[AthenaConfig.sql_instrument_field] = instrument

            # look at timestamp.
            ft = int(row[dt_field])

            # if it lies before merged point, select as history
            if ft < merge_point_ft or coerced_replay:
                cleaned_row = HermesDataHandler.__clean_data(
                    row, is_hash_set=True)
                if not is_kline:
                    self.publish_md(cleaned_row)
                else:
                    self.publish_kline(cleaned_row)
            if coerced_replay and HermesDataHandler.replay_time_interval:
                time.sleep(HermesDataHandler.replay_time_interval)

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

    def distribute_data(self):
        """

        :return:
        """
        for message in self.sub.listen():
            if message['type'] == 'message':

                # clean data
                try:
                    cleaned_data = HermesDataHandler.__clean_data(message)
                except UnicodeError:
                    # catch the unicode error.
                    print('[Data Handler]: Broken unicode sequence in '
                          'message: {}.'.format(message))
                    continue

                instrument = cleaned_data[AthenaConfig.sql_instrument_field]

                # cleaned data is kline or md
                is_kline = True if 'close' in cleaned_data else False

                if not is_kline:
                    # recover md history
                    if not self.md_history_fixed[instrument]:
                        merge_point = \
                            cleaned_data[AthenaConfig.sql_local_dt_field]
                        self.__fix_history(merge_point, instrument)
                        self.md_history_fixed[instrument] = True

                    # publish cleaned md data
                    flag = self.publish_md(cleaned_data)
                else:
                    # recover kline history
                    if not self.kline_history_fixed[instrument]:
                        merge_point = \
                            cleaned_data[
                                AthenaConfig.sql_kline_open_time_field]
                        self.__fix_history(merge_point, instrument,
                                           is_kline=True)
                        self.kline_history_fixed[instrument] = True

                    # publish cleaned md data
                    flag = self.publish_kline(cleaned_data)

    def replay(self, md=True, kline=True):
        """
        replay all the data cached in db0
        :param md: boolean, whether to replay md data.
        :param kline: boolean, replay kline or not
        :return:
        """
        if md:
            for instrument in self.instruments_list:
                if not self.md_history_fixed[instrument]:
                    self.__fix_history(None, instrument, coerced_replay=True)
                    self.md_history_fixed[instrument] = True

        if kline:
            for instrument in self.instruments_list:
                if not self.kline_history_fixed[instrument]:
                    self.__fix_history(None, instrument,
                                       is_kline=True, coerced_replay=True)
                    self.kline_history_fixed[instrument] = True


def clean_hermes_data(message, is_hash_set=False):
    """
    clean hermes data message.
    :param message: message received from sub channel.
    :param is_hash_set: bool, specifies whether the message is a dict
    from sub.listen() or that from RedisWrapper.get_dict()
        - True: from RedisWrapper.get_dict()
        - False: from sub.listen()
    Default is False.
    :return:
    """
    # convert types
    if not is_hash_set:
        str_message = message['data'].decode('utf-8')
        list_message = str_message.split(AthenaConfig.hermes_md_sep_char)
        dict_message = dict(zip(list_message[0::2], list_message[1::2]))
    else:
        dict_message = message
    try:
        if 'bid1' in dict_message:     # if tick (md) message

            # clean tick data
            ex_update_time = filetime_to_dt(
                int(dict_message['systime']))
            local_update_time = filetime_to_dt(
                int(dict_message['subtime']))

            if not is_hash_set:
                # contract field is settled here
                # only when from sub.listen()
                contract = dict_message['key'].split(':')[0].split('.')[-1]
            else:
                contract = dict_message[AthenaConfig.sql_instrument_field]

            bid = float(dict_message['bid1']) / 10000
            ask = float(dict_message['ask1']) / 10000
            last_price = float(dict_message['lastprice']) / 10000

            # make dict_data
            dict_data = {
                'trade_day': dict_message['day'],
                'ex_update_time': ex_update_time,
                'local_update_time': local_update_time,
                'contract': contract,
                'ask': ask,
                'bid': bid,
                'ask_vol': dict_message['askvol1'],
                'bid_vol': dict_message['bidvol1'],
                'volume': dict_message['volume'],
                'open_interest': dict_message['openinterest'],
                'last_price': last_price
            }

        elif 'openwndtime' in dict_message:      # kline message

            # clean kline data
            ex_update_time = filetime_to_dt(
                int(dict_message['opensystime']))
            local_update_time = filetime_to_dt(
                int(dict_message['openwndtime']))

            if not is_hash_set:
                # contract field is settled here
                # only when from sub.listen()
                contract = dict_message['key'].split(':')[0].split('.')[-2]
            else:
                contract = dict_message[AthenaConfig.sql_instrument_field]

            open_price = float(dict_message['open']) / 10000
            high_price = float(dict_message['high']) / 10000
            low_price = float(dict_message['low']) / 10000
            close_price = float(dict_message['close']) / 10000

            # make dict_data
            dict_data = {
                'ex_open_time': ex_update_time,
                'open_time': local_update_time,
                'contract': contract,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': dict_message['volume'],
                'open_interest': dict_message['openinterest'],
                'duration': dict_message['dur'],
                'tag': 'kline'
            }

        else:
            raise KeyError
    except KeyError:
        # when there is some fields missing
        print('[Data Handler]: Incomplete message skipped.')
        return dict()
    return dict_data