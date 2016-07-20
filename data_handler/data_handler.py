import json
import time
from abc import ABCMeta, abstractmethod

from Athena.settings import AthenaConfig
from Athena.utils import append_digits_suffix_for_redis_key
from Athena.db_wrappers.redis_wrapper import RedisWrapper
from Athena.data_handler.clean_data import \
    make_hermes_single_instrument_stream, clean_hermes_data

Tf, Kf = AthenaConfig.TickFields, AthenaConfig.KLineFields

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
    _replay_interval = 1

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
            this_instrument = data[Tf.contract]
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
            data[Tf.ex_time] = data[Tf.ex_time].strftime(
                AthenaConfig.dt_format)
            data[Tf.local_time] = data[Tf.local_time].strftime(
                AthenaConfig.dt_format)

            message = json.dumps({athena_unique_key: data})
            self.pub_wrapper.connection.publish(
                channel=pub_channel,
                message=message
            )

            # increment to counter
            self.counters['md'][this_instrument] += 1
            return 1

        # If data is of type kline
        elif data['tag'] == AthenaConfig.AthenaMessageTypes.kl:

            # find instrument and duration specifier, map to pub channel
            this_instrument = data[Kf.contract]
            dur_specifier = data[Kf.duration_specifier]
            pub_channel = \
                self.pub_channels['kl'][this_instrument][dur_specifier]

            # map to new key in Athena db
            athena_unique_key = append_digits_suffix_for_redis_key(
                prefix=pub_channel,
                counter=self.counters['kl'][this_instrument][dur_specifier]
            )

            # append count field to bar data
            data[Kf.count] = (
                self.counters['kl'][this_instrument][dur_specifier]
            )

            # publish dict data
            self.pub_wrapper.set_dict(athena_unique_key, data)

            # publish str message
            # first serialize datetime fields (ex_open, open and close time)
            data[Kf.ex_open_time] = data[Kf.ex_open_time].strftime(
                AthenaConfig.dt_format)
            data[Kf.open_time] = data[Kf.open_time].strftime(
                AthenaConfig.dt_format)
            data[Kf.end_time] = data[Kf.end_time].strftime(
                AthenaConfig.dt_format)

            message = json.dumps({athena_unique_key: data})
            self.pub_wrapper.connection.publish(
                channel=pub_channel,
                message=message
            )

            # publish plotting message
            pub_channel_plot = \
                self.pub_channels['kl_plot'][this_instrument][dur_specifier]

            # map to new key in Athena db (plotting)
            athena_unique_key_plotting = append_digits_suffix_for_redis_key(
                prefix=pub_channel_plot,
                counter=self.counters['kl'][this_instrument][dur_specifier]
            )

            # publish plotting (dict) data
            self.pub_wrapper.set_dict(athena_unique_key_plotting, data)

            # publish plotting str message.
            plot_message = json.dumps({athena_unique_key_plotting: data})
            self.pub_wrapper.connection.publish(
                channel=pub_channel_plot,
                message=plot_message
            )

            # increment to counter
            self.counters['kl'][this_instrument][dur_specifier] += 1
            return 1

    def add_instrument(self, instrument, kline_dur_specifiers=('1m',)):
        """
        Begin to listen to one single instrument.
        :param instrument: string
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
            self.pub_channels['kl_plot'][instrument][dur] = \
                'plot:kl:' + instrument + '.' + dur

        # initialize counters
        self.counters['md'][instrument] = 0
        self.counters['kl'][instrument] = dict()
        for dur in kline_dur_specifiers:
            self.counters['kl'][instrument][dur] = 0

        # add instrument to subscribed list
        self.subscribed_instruments.append(instrument)

    def replay_single_instrument(self, instrument,
                                 kline_dur_specifiers='1m',
                                 stream='keys',
                                 show_time=True):
        """
        Replay the redis cached data (mixed both stream of md and k-lines)
        of one single instrument, simulating data stream that was pushed
        by Hermes in real time.

        :param instrument: string
        :param kline_dur_specifiers: string.
            Default is '1m', md is mixed with 1min k-lines.

        :param stream: string, the mode to mix md and kline data stream.
            - 'transport': mix and transport stream to another db. There will
                be an extra transporting stage before replaying. Transporting
                stage will handle unicode errors before replaying stage
                (the signals, plots and strategies are active only in replaying
                 stage).
            - 'keys': mix by keys, don't make another cache. Must
                handle unicode error itself when replaying.
            - 'existed': use cached stream created by 'transport' mode. There
                must be an existed stream data repo in db targeted by
                hist_wrapper.
            - Default mode is 'keys'.
            The running time of 'keys' and 'existed' are almost same.
            'transport' will cost more time in the extra stage.

        :param show_time: bool, whether to show time spent replaying.
        :return:
        """
        # if the instrument is not subscribed
        if instrument not in self.subscribed_instruments:
            print('[Data Handler]: Not subscribed to {}.'.format(
                instrument))
            return

        start_time = time.time()

        print('[Data Handler]: Mixing md and kline data stream.')

        # Hermes cached directory
        md_dir = HermesDataHandler._md_map[instrument]
        kl_dir = HermesDataHandler._kl_map[kline_dur_specifiers][instrument]

        # make stream by different mode.
        if stream == 'transport':
            replayed_keys = make_hermes_single_instrument_stream(
                md_dir, kl_dir, transport=True
            )
            wrapper = self.hist_wrapper
        elif stream == 'keys':
            replayed_keys = make_hermes_single_instrument_stream(
                md_dir, kl_dir, transport=False
            )
            wrapper = self.sub_wrapper
        elif stream == 'existed':
            replayed_keys = self.hist_wrapper.get_keys('{}:*'.format(
                AthenaConfig.redis_temp_hist_stream_dir))
            wrapper = self.hist_wrapper
        else:
            raise ValueError

        # begin replaying
        print('[Data Handler]: Start replaying.')

        # flush Athena db
        self.pub_wrapper.flush_db()

        num_keys = len(replayed_keys)

        # pop row from historical data stream
        for k in replayed_keys:
            try:
                row = wrapper.get_dict(k)
            except UnicodeError:
                print('[Data Handler]: Unicode error at key {}.'.format(k))
                continue

            # clean data (hash set)
            cleaned_row = clean_hermes_data(
                row, is_hash_set=True, this_contract=instrument)

            # publish data
            self.__publish(cleaned_row)

        # end of replaying, show a statistic
        end_time = time.time()
        if show_time:
            print('[Data Handler]: Finished replaying {} rows'
                  ' in {} seconds.'.format(
                        num_keys, end_time-start_time)
            )

