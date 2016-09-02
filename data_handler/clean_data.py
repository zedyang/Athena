from datetime import datetime
from Athena.utils import filetime_to_dt
from Athena.settings import AthenaConfig

HTf, HKf = AthenaConfig.HermesTickFields, AthenaConfig.HermesKLineFields

__author__ = 'zed'


# -------------------------------------------------------------------------
def clean_hermes_md_data(raw, is_hash_set=False):
    """
    clean hermes md data.
    :param raw: data received from sub channel, published by Hermes.

    :param is_hash_set: bool, specifies whether the message is a dict
    from sub.listen() or that from RedisWrapper.get_dict()
        - True: from RedisWrapper.get_dict()
        - False: from sub.listen()
        - Default is False.

    :return:
    """
    # convert types
    if not is_hash_set:
        str_message = raw['data'].decode('utf-8')
        l = str_message.split(AthenaConfig.hermes_md_sep_char)
        d = dict(zip(l[0::2], l[1::2]))
    else:
        d = raw

    # convert time fields to dt.
    d[HTf.ex_time] = filetime_to_dt(
        int(d[HTf.ex_time]))
    d[HTf.local_time] = filetime_to_dt(
        int(d[HTf.local_time]))

    # rescale float fields
    for field in HTf.floats:
        d[field] = float(d[field]) / 10000

    # convert integer fields
    for field in HTf.integers:
        d[field] = int(d[field])

    # modify other fields
    # get instrument name from key.
    parsed_key = d[HTf.key].split(':')[0].split('.')
    if len(parsed_key) == 3:
        contract = parsed_key[-1]
    elif len(parsed_key) == 4:  # Au99.99, an extra '.'
        contract = '.'.join([parsed_key[-2], parsed_key[-1]])
    else:
        contract = parsed_key[-1]

    d[HTf.contract] = contract
    d['tag'] = AthenaConfig.AthenaMessageTypes.md

    return d


# -------------------------------------------------------------------------
def clean_hermes_kl_data(raw, is_hash_set=False):
    """
    clean hermes kl data.
    :param raw: data received from sub channel, published by Hermes.

    :param is_hash_set: bool, specifies whether the message is a dict
    from sub.listen() or that from RedisWrapper.get_dict()
        - True: from RedisWrapper.get_dict()
        - False: from sub.listen()
        - Default is False.

    :return:
    """
    # convert types
    if not is_hash_set:
        str_message = raw['data'].decode('utf-8')
        l = str_message.split(AthenaConfig.hermes_md_sep_char)
        d = dict(zip(l[0::2], l[1::2]))
    else:
        d = raw

    # convert time fields to dt.
    for field in HKf.times:
        try:
            d[field] = filetime_to_dt(int(d[field]))
        except OSError:
            d[field] = filetime_to_dt(116444736000000000)

    # rescale float fields
    for field in HKf.floats:
        d[field] = float(d[field]) / 10000

    # convert integer fields
    for field in HKf.integers:
        d[field] = int(d[field])

    # modify other fields
    # get instrument name from key.
    parsed_key = d[HKf.key].split(':')[0].split('.')
    if len(parsed_key) == 4:
        contract = parsed_key[-2]
    elif len(parsed_key) == 5:  # Au99.99, an extra '.'
        contract = '.'.join([parsed_key[-3], parsed_key[-2]])
    else:
        contract = parsed_key[-1]

    d[HKf.contract] = contract
    d[HKf.duration_specifier] = \
        AthenaConfig.hermes_kl_seconds_to_dur[d[HKf.duration]]
    d['tag'] = AthenaConfig.AthenaMessageTypes.kl

    return d


# -------------------------------------------------------------------------
def mix_hermes_md_kl_keys(md_keys_dict, kl_keys_dict):
    """

    :param md_keys_dict: dict, in the form of {instrument: [list of keys], ...}
    :param kl_keys_dict: dict,
        in the form of {instrument: {dur: [list of keys], ...}}
    :return:
    """
    all_keys = []
    # make (key, time) tuples list for md.
    for inst in md_keys_dict:
        keys_this_inst = [(k, int(k.decode('utf8').split(":")[1]))
                          for k in md_keys_dict[inst]]
        all_keys.extend(keys_this_inst)

    all_kl_keys = []
    # make (key, time) tuples list for kl.
    for inst in kl_keys_dict:
        for dur in kl_keys_dict[inst]:
            keys = [(k, int(k.decode('utf8').split(":")[1]))
                    for k in kl_keys_dict[inst][dur]]
            all_kl_keys.extend(keys)
    all_keys.extend(all_kl_keys)

    # sort by time
    sorted_keys = sorted(
        all_keys,
        key=lambda tup: tup[1]
    )

    sorted_keys = [k[0] for k in sorted_keys]
    return sorted_keys



