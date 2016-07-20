from collections import deque

from Athena.utils import filetime_to_dt, append_digits_suffix_for_redis_key
from Athena.settings import AthenaConfig
from Athena.db_wrappers.redis_wrapper import RedisWrapper
Tf, Kf = AthenaConfig.TickFields, AthenaConfig.KLineFields
HTf, HKf = AthenaConfig.HermesTickFields, AthenaConfig.HermesKLineFields

__author__ = 'zed'


# -------------------------------------------------------------------------
def clean_hermes_data(raw, is_hash_set=False, this_contract=None):
    """
    clean hermes data.
    :param raw: data received from sub channel, published by Hermes.

    :param is_hash_set: bool, specifies whether the message is a dict
    from sub.listen() or that from RedisWrapper.get_dict()
        - True: from RedisWrapper.get_dict()
        - False: from sub.listen()
        - Default is False.

    :param this_contract: string, the contract name. The contract can be got
        when raw is from sub.listen() message, Hence when is_hash_set = False
        It is not necessary to specify this_contract.
        However, if raw is a hash set, there is no contract field within,
        this_contract parameter must be specified, otherwise the contract field
        will also be None in cleaned data (return).

    :return: cleaned data that matches AthenaConfig.TickFields or KLineFields.
    """
    # convert types
    if not is_hash_set:
        str_message = raw['data'].decode('utf-8')
        l = str_message.split(AthenaConfig.hermes_md_sep_char)
        d = dict(zip(l[0::2], l[1::2]))
    else:
        d = raw
    try:
        # if tick (md) message
        if HTf.bid_vol in d:

            # clean tick data

            # convert time fields to dt.
            ex_update_time = filetime_to_dt(
                int(d[HTf.ex_time]))
            local_update_time = filetime_to_dt(
                int(d[HTf.local_time]))

            if not is_hash_set:
                # contract field is settled here
                # only when from sub.listen()
                contract = d['key'].split(':')[0].split('.')[-1]
            else:
                contract = this_contract

            # rescale float fields
            bid = float(d[HTf.bid]) / 10000
            ask = float(d[HTf.ask]) / 10000
            last_price = float(d[HTf.last_price]) / 10000
            open_int = float(d[HTf.open_int]) / 10000

            # make dict_data
            dict_data = {
                Tf.day: d[HTf.day],
                Tf.ex_time: ex_update_time,
                Tf.local_time: local_update_time,
                Tf.contract: contract,
                Tf.ask: ask,
                Tf.bid: bid,
                Tf.ask_vol: int(d[HTf.ask_vol]),
                Tf.bid_vol: int(d[HTf.bid_vol]),
                Tf.volume: int(d[HTf.volume]),
                Tf.open_int: open_int,
                Tf.last_price: last_price,
                'tag': AthenaConfig.AthenaMessageTypes.md
            }

        elif HKf.open_time in d:      # kline message

            # clean kline data
            duration = int(d[HKf.duration])

            # catch empty time field
            if int(d[HKf.ex_open_time]) == 0:
                raise ValueError

            # convert time fields to dt.
            ex_open_time = filetime_to_dt(
                int(d[HKf.ex_open_time]))
            open_time = filetime_to_dt(
                int(d[HKf.open_time]))
            end_time = filetime_to_dt(
                int(d[HKf.open_time]) + 10000000 * duration
            )

            if not is_hash_set:
                # contract field is settled here
                # only when from sub.listen()
                contract = d['key'].split(':')[0].split('.')[-2]
            else:
                contract = this_contract

            # rescale float fields
            open_price = float(d[HKf.open_price]) / 10000
            high_price = float(d[HKf.high_price]) / 10000
            low_price = float(d[HKf.low_price]) / 10000
            close_price = float(d[HKf.close_price]) / 10000

            # duration and specifier
            duration = int(d[HKf.duration])
            duration_specifier = \
                AthenaConfig.hermes_kl_seconds_to_dur[duration]

            # make dict_data
            dict_data = {
                Kf.ex_open_time: ex_open_time,
                Kf.open_time: open_time,
                Kf.end_time: end_time,
                Kf.contract: contract,
                Kf.open_price: open_price,
                Kf.high_price: high_price,
                Kf.low_price: low_price,
                Kf.close_price: close_price,
                Kf.volume: d[HKf.volume],
                Kf.open_int: d[HKf.open_int],
                Kf.duration: duration,
                Kf.duration_specifier: duration_specifier,
                'tag': AthenaConfig.AthenaMessageTypes.kl
            }

        else:
            raise KeyError
    except KeyError:
        # when there is some fields missing
        print('[Data Handler]: Incomplete message skipped.')
        return dict()
    return dict_data


# -------------------------------------------------------------------------
def make_hermes_single_instrument_stream(md_dir, kline_dir,
                                         kline_dur=60,
                                         merge_point=None,
                                         transport=True):
    """
    Sort hermes md and kline data of one instrument
    according to time sequence and then interlace them together into one deck.

    The purpose of making such stream is that iterating through this data
    stream simulates the real sequence of data pushing in real time.

    :param md_dir: string, directory of ticks
    :param kline_dir: string, directory of bars.

    :param kline_dur: integer, the duration field of kline.
        - default value is 60 seconds.

    :param merge_point: False or integer. If specified, marks the
        end point of stream.
        The meaning of integer is the corresponding filetime field in hermes
        raw data.
        - default value: False, no end point specified, restoring all data in
        md_dir and kline_dir.

    :param transport: bool, whether to transport stream to hist_db.
        - True: transport stream by pub_wrapper.
        - False: Not add keys into redis, only return sorted keys.
        - default value: True
    :return:
    """
    ft_delta_second = 10000000

    # make two connections
    sub_wrapper = RedisWrapper(db=AthenaConfig.hermes_db_index)
    pub_wrapper = RedisWrapper(db=AthenaConfig.hist_stream_db_index)

    # get list of keys
    md_keys = sub_wrapper.get_keys('{}:*'.format(md_dir))

    # kline keys are pushed in a deque.
    kl_keys = deque(sub_wrapper.get_keys('{}:*'.format(kline_dir)))

    # prepare sorted keys container
    sorted_keys = []
    # iterate through md and insert kline at appropriate positions
    for k in md_keys:
        try:
            md_time = int(k.decode('utf-8').split(':')[-1])

            # examine leftmost key in kline deque.
            next_kl_start = int(kl_keys[0].decode('utf-8').split(':')[-1])
            next_kl_end = next_kl_start + kline_dur * ft_delta_second

            # if tick exceed current kline, insert kline before this tick.
            if md_time > next_kl_end:
                kk = kl_keys.popleft()

                # append bar if it is earlier than merge point.
                if not merge_point or next_kl_end < merge_point:
                    sorted_keys.append(kk)

            # append the tick if it is earlier than merge point
            if not merge_point or md_time < merge_point:
                sorted_keys.append(k)
        except UnicodeError:
            print('[Data Handler]: Unicode error at key {}.'.format(k))
            continue

    # begin to publish the stream to new repo.
    if sorted_keys and transport:

        # flush target db.
        pub_wrapper.flush_db()

        counter = 0
        # iterate through sorted keys.
        for k in sorted_keys:

            # get dict data.
            try:
                dict_data = sub_wrapper.get_dict(k)
            except UnicodeError:
                print('[Data Handler]: Unicode error at key {}.'.format(k))
                continue

            # map to new key.
            new_key_in_redis = append_digits_suffix_for_redis_key(
                prefix=AthenaConfig.redis_temp_hist_stream_dir,
                counter=counter
            )

            # set data in redis
            pub_wrapper.set_dict(
                key=new_key_in_redis,
                data=dict_data
            )
            # increment to counter
            counter += 1

    return sorted_keys
