import json
import time
from abc import ABCMeta, abstractmethod
from datetime import datetime

from Athena.settings import AthenaConfig
from Athena.trade_time import is_in_trade_time
from Athena.utils import append_digits_suffix_for_redis_key
from Athena.data_handler.redis_wrapper import RedisWrapper
from Athena.data_handler.clean_data import clean_hermes_md_data, \
    clean_hermes_kl_data, mix_hermes_md_kl_keys

HTf, HKf = AthenaConfig.HermesTickFields, AthenaConfig.HermesKLineFields

__author__ = 'zed'


# -----------------------------------------------------------------------
class DataHandler(object):
    """
    The abstract class that provides interfaces to specific data handler
    objects implementations.

    A data handler models a pub-sub entity with respect to Redis database.
    It subscribes and listens to some channels, perform some operations
    on the retrieved data and then publish the data to other channels.

    The data handler may open multiple connections to redis database,
    especially when the pub/sub channels are in different dbs.

    Concrete classes that inherits DataHandler should implement protected
    methods __subscribe() and __publish(data).
    The former is to open connections to redis and subscribe to sub channels
    The latter is to publish processed data to db.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def __subscribe(self, channels):
        """

        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def __publish(self, data):
        """

        :return:
        """
        raise NotImplementedError


# -----------------------------------------------------------------------
class HermesDataHandler(DataHandler):
    """
    Hermes data handler class.
    It is responsible for wrapping the real-time data stream pushed by
    Hermes server to a structure utilized by Athena modules.

    The HermesDataHandler opens three connections to redis database. One
    is targeting at Hermes db (sub_wrapper, currently db0), second is targeting
    at temp history stream db (hist_wrapper, currently db3), third is targeting
    at Athena db (pub_wrapper, currently db1).
    These three connections are responsible for listening to Hermes message(0),
    retrieve historical data stream(3) and publishing processed data(1)
    respectively. The connections are opened on construction of handler
    instances.

    class attributes:
    ----------------
    * _code_book: AthenaConfig.HermesInstrumentsList class. Contains the
        list of instruments that each data API could subscribe to.

    * _md_map/_kl_map: AthenaConfig.hermes_#_mapping dictionary.
        The mapping from instrument to corresponding data directory in redis.
        - _md_map has one layer: _md_map[instrument]
        - _kl_map has two layers: _kl_map[dur][instrument]

    * _replay_interval: the interval between iterations of hist data replay
        1/interval is replaying frequency. (# data records in 1 second)

    public attributes:
    ----------------
    * subscribed_instruments: list of subscribed instruments.

    * pub_channels: json dict. The publishing channels of md and k-line data,
        targeting at Athena_db (currently db1).
        {
            'md': {
                'Au(T+D)': 'md:Au(T+D)',
                'GC1608': 'md:GC1608',
                'otherInst{#1}': 'md:#1',
                ...
            },
            'kl': {
                'Au(T+D)': {
                    '1m': 'kl:Au(T+D).1m',
                    '3m': 'kl:Au(T+D).3m',
                },
                'GC1608': {
                    'clock': 'kl:GC1608.clock'
                },
                'otherInst{#1}': {
                    'clock': 'kl:#1.clock'
                    '1m': 'kl:#1.1m',
                    'otherDur{#2}': 'kl:#1.#2',
                },
                ...
            },
            'kl_plot': {
                'Au(T+D)': {
                    '1m': 'plot:kl:Au(T+D).1m',
                    '3m': 'plot:kl:Au(T+D).3m',
                },
                'GC1608': {
                    'clock': 'plot:kl:GC1608.clock'
                },
                'otherInst{#1}': {
                    'clock': 'plot:kl:#1.clock'
                    '1m': 'plot:kl:#1.1m',
                    'otherDur{#2}': 'plot:kl:#1.#2',
                },
                ...
            }
        }
        pub channels of new instruments are updated when added into
        subscribed instruments list. 'md' has one layer [inst]. 'kl' has two
        layers [inst][dur].

    * counters: json dict. The counters starting from 0 to make keys and
        indices of data records in Athena db. The counters dict has same
        hierarchies as pub_channels dict.

    protected methods:
    ----------------
    * __subscribe(self): Implements abstract method of data handler interface.

    * __publish(self): Implements abstract method of data handler interface.

    public methods:
    ----------------
    * add_instrument(self, instrument): public wrapper of subscribe protected
        method. Let the sub connection wrapper listen to the specified
        instrument.

    *  replay_single_instrument(self, instrument, kline_dur_specifiers,
                                stream, show_time):
    """
    # current instrument code book.
    _code_book = AthenaConfig.HermesInstrumentsList

    # instrument -> data directory mapping
    _md_map = AthenaConfig.hermes_md_mapping
    _kl_map = AthenaConfig.hermes_kl_mapping

    # replaying time interval
    _replay_interval = 0

    def __init__(self):
        """
        constructor.
        """
        # open connections to redis server.
        self.sub_wrapper = RedisWrapper(db=AthenaConfig.hermes_db_index)
        self.hist_wrapper = RedisWrapper(db=AthenaConfig.hist_stream_db_index)
        self.pub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)

        # create a listener
        self.sub = self.sub_wrapper.connection.pubsub()

        # prepare subscribe/publishing list
        self.subscribed_instruments = []
        self.pub_channels = {
            'md': dict(),
            'kl': dict(),
            'kl_plot': dict()
        }

        # prepare counters to make redis keys
        self.counters = {
            'md': dict(),
            'kl': dict()
        }

    def __subscribe(self, channels):
        """

        :return:
        """
        self.sub.subscribe(channels)

    def __publish(self, data):
        """
        publish (cleaned) data into redis db.
        :param data:
        :return:
        """
        # If data is of type md:
        if data['tag'] == AthenaConfig.AthenaMessageTypes.md:

            # find instrument and map to pub channel
            this_instrument = data[HTf.contract]
            pub_channel = self.pub_channels['md'][this_instrument]

            # map to new key in Athena db
            athena_unique_key = append_digits_suffix_for_redis_key(
                prefix=pub_channel,
                counter=self.counters['md'][this_instrument]
            )

            # publish dict data
            self.pub_wrapper.set_dict(athena_unique_key, data)

            # publish str message
            # first serialize datetime fields (ex and local time)
            data[HTf.ex_time] = data[HTf.ex_time].strftime(
                AthenaConfig.dt_format)
            data[HTf.local_time] = data[HTf.local_time].strftime(
                AthenaConfig.dt_format)

            message = json.dumps({athena_unique_key: data})
            self.pub_wrapper.connection.publish(
                channel=pub_channel,
                message=message
            )

            # update the one record for storing last md
            # note that this 'current' can only be retrieved subjectively
            athena_unique_key_current = str(pub_channel) + ':0'
            self.pub_wrapper.set_dict(athena_unique_key_current, data)

            # increment to counter
            self.counters['md'][this_instrument] += 1
            return 1

        # If data is of type kline
        elif data['tag'] == AthenaConfig.AthenaMessageTypes.kl:

            # find instrument and duration specifier, map to pub channel
            this_instrument = data[HKf.contract]
            dur_specifier = data[HKf.duration_specifier]
            pub_channel = \
                self.pub_channels['kl'][this_instrument][dur_specifier]

            # map to new key in Athena db
            athena_unique_key = append_digits_suffix_for_redis_key(
                prefix=pub_channel,
                counter=self.counters['kl'][this_instrument][dur_specifier]
            )

            # append count field to bar data
            data[HKf.count] = (
                self.counters['kl'][this_instrument][dur_specifier]
            )

            # publish dict data
            self.pub_wrapper.set_dict(athena_unique_key, data)

            # publish str message
            # first serialize datetime fields (ex_open, open and close time)
            for field in HKf.times:
                if type(data[field]) == datetime:
                    data[field] = data[field].strftime(
                        AthenaConfig.dt_format)

            message = json.dumps({athena_unique_key: data})
            self.pub_wrapper.connection.publish(
                channel=pub_channel,
                message=message
            )

            # publish plotting message
            if type(self.pub_channels['kl_plot']
                    [this_instrument][dur_specifier]) == str:
                pub_channel_plot = \
                    self.pub_channels['kl_plot'][
                        this_instrument][dur_specifier]

                # map to new key in Athena db (plotting)
                athena_unique_key_plotting = \
                    append_digits_suffix_for_redis_key(
                        prefix=pub_channel_plot,
                        counter=self.counters['kl'][
                            this_instrument][dur_specifier]
                    )

                # publish plotting (dict) data
                self.pub_wrapper.set_dict(
                    athena_unique_key_plotting, data)

                # publish plotting str message.
                plot_message = json.dumps(
                    {athena_unique_key_plotting: data}
                )
                self.pub_wrapper.connection.publish(
                    channel=pub_channel_plot,
                    message=plot_message
                )

            elif type(self.pub_channels['kl_plot'][
                        this_instrument][dur_specifier]) == list:

                for i in range(len(self.pub_channels['kl_plot'][
                        this_instrument][dur_specifier])):

                    pub_channel_plot = \
                        self.pub_channels['kl_plot'][
                            this_instrument][dur_specifier][i]

                    # map to new key in Athena db (plotting)
                    athena_unique_key_plotting = \
                        append_digits_suffix_for_redis_key(
                            prefix=pub_channel_plot,
                            counter=self.counters['kl'][
                                this_instrument][dur_specifier]
                        )

                    # publish plotting (dict) data
                    self.pub_wrapper.set_dict(
                        athena_unique_key_plotting, data)

                    # publish plotting str message.
                    plot_message = json.dumps(
                        {athena_unique_key_plotting: data}
                    )
                    self.pub_wrapper.connection.publish(
                        channel=pub_channel_plot,
                        message=plot_message
                    )

            # update the one record for storing last kl
            athena_unique_key_current = str(pub_channel) + ':0'
            self.pub_wrapper.set_dict(athena_unique_key_current, data)

            # increment to counter
            self.counters['kl'][this_instrument][dur_specifier] += 1
            return 1

    def add_instrument(self, instrument, kline_dur_specifiers,
                       duplicate=1):
        """
        Begin to listen to one single instrument.
        :param instrument: string
        :param duplicate:
        :param kline_dur_specifiers: tuple of strings.
            Default is ('1m'), subscribe 1 minute kline only.
        :return:
        """
        # if the instrument already subscribed
        if instrument in self.subscribed_instruments:
            print('[Data Handler]: Already listening to {}.'.format(
                instrument))
            return
        # if instrument is not distinguishable:
        if instrument not in HermesDataHandler._code_book.all:
            print('[Data Handler]: {} is not in Hermes code book.'.format(
                instrument))
            return

        # otherwise
        channels = [HermesDataHandler._md_map[instrument]]

        for dur in kline_dur_specifiers:
            channels.append(HermesDataHandler._kl_map[dur][instrument])

        # subscribe to channels
        self.__subscribe(channels)

        # update publishing channels
        self.pub_channels['md'][instrument] = 'md:' + instrument
        self.pub_channels['kl'][instrument] = dict()
        self.pub_channels['kl_plot'][instrument] = dict()
        for dur in kline_dur_specifiers:
            self.pub_channels['kl'][instrument][dur] = \
                'kl:' + instrument + '.' + dur
            if duplicate == 1:
                self.pub_channels['kl_plot'][instrument][dur] = \
                    'plot:kl:' + instrument + '.' + dur
            elif duplicate > 1:
                self.pub_channels['kl_plot'][instrument][dur] = []
                for i in range(duplicate):
                    self.pub_channels['kl_plot'][instrument][dur].append(
                        'plot_{}:kl:'.format(i) + instrument + '.' + dur
                    )

        # initialize counters
        self.counters['md'][instrument] = 0
        self.counters['kl'][instrument] = dict()
        for dur in kline_dur_specifiers:
            self.counters['kl'][instrument][dur] = 0

        # add instrument to subscribed list
        self.subscribed_instruments.append(instrument)

    def replay_data(self,
                    clean_up=True,
                    attach_end_flag=False):
        """

        :param kline_dur_specifier:
        :param clean_up:
        :param attach_end_flag:
        :return:
        """
        start_time = time.time()

        md_keys_to_mix = dict()
        kl_keys_to_mix = dict()

        for inst in self.subscribed_instruments:
            # Hermes cached directory for md
            md_dir = HermesDataHandler._md_map[inst]
            # retrieve md keys
            md_keys = self.sub_wrapper.get_keys('{}:*'.format(md_dir))
            md_keys_to_mix[inst] = md_keys

            kl_keys_to_mix[inst] = dict()
            for dur in self.pub_channels['kl'][inst]:
                # Hermes cached directory for kl
                kl_dir = HermesDataHandler._kl_map[dur][inst]
                # retrieve kl keys
                kl_keys = self.sub_wrapper.get_keys('{}:*'.format(kl_dir))
                kl_keys_to_mix[inst][dur] = kl_keys

        # sort the keys
        sorted_keys = mix_hermes_md_kl_keys(
            md_keys_to_mix, kl_keys_to_mix
        )

        # flush Athena db
        if clean_up:
            self.pub_wrapper.flush_db()

        num_keys = len(sorted_keys)

        # pop row from historical data stream
        for k in sorted_keys:
            try:
                row = self.sub_wrapper.get_dict(k)
            except UnicodeError:
                print('[Data Handler]: Unicode error at key {}.'.format(k))
                continue

            # contract name
            if b'md' in k:

                # clean data (hash set)
                cleaned_row = clean_hermes_md_data(row, is_hash_set=True)
                update_time = cleaned_row[HTf.ex_time]
                contract = cleaned_row[HTf.contract]

            elif b'kl' in k:
                # clean data (hash set)
                try:
                    cleaned_row = clean_hermes_kl_data(row, is_hash_set=True)
                except OSError:
                    continue
                update_time = cleaned_row[HKf.ex_time]
                contract = cleaned_row[HKf.contract]

            else:
                print(k)
                raise ValueError

            if is_in_trade_time(update_time, contract):
                # publish data
                self.__publish(cleaned_row)

            if self._replay_interval:
                time.sleep(self._replay_interval)

        if attach_end_flag:
            time.sleep(1)
            # publish end flag
            end_flag = {
                'tag': 'flag',
                'type':'flag_0'
            }
            end_message = json.dumps({'flags:0': end_flag})
            self.pub_wrapper.connection.publish(
                channel='flags',
                message=end_message
            )

        # end of replaying, show a statistic
        end_time = time.time()
        print('[Data Handler]: Finished replaying {} rows'
              ' in {} seconds.'.format(
                num_keys, end_time - start_time)
        )

    def distribute_data(self):
        """

        :return:
        """
        fixed_history = False
        for message in self.sub.listen():
            if message['type'] == 'message':

                # fix history
                if not fixed_history:
                    self.replay_data()
                    fixed_history = True

                # clean data
                try:
                    # decompress message to dict
                    str_message = message['data'].decode('utf-8')
                    l = str_message.split(AthenaConfig.hermes_md_sep_char)
                    d = dict(zip(l[0::2], l[1::2]))

                    if HKf.close_time in d:
                        # clean kl data
                        cleaned_data = clean_hermes_kl_data(
                            d, is_hash_set=True)
                        update_time = cleaned_data[HKf.ex_time]
                        contract = cleaned_data[HKf.contract]

                    elif HTf.bid_vol_1 in d:
                        # clean tick data
                        cleaned_data = clean_hermes_md_data(
                            d, is_hash_set=True)
                        update_time = cleaned_data[HTf.ex_time]
                        contract = cleaned_data[HTf.contract]

                    else: raise ValueError

                except UnicodeError:
                    # catch the unicode error.
                    print('[Data Handler]: Broken unicode sequence in '
                          'message: {}.'.format(message))
                    continue

                if is_in_trade_time(update_time, contract):
                    self.__publish(cleaned_data)

