from datetime import datetime
from Athena.data_handler.sql_wrapper import SQLWrapper
from Athena.data_handler.redis_wrapper import RedisWrapper
from Athena.settings import AthenaConfig
from Athena.utils import dt_to_filetime
HKf, SKf = AthenaConfig.HermesKLineFields, AthenaConfig.SQLKlineFields
HTf, STf = AthenaConfig.HermesTickFields, AthenaConfig.SQLTickFields


def make_dump(symbols, begin_time, end_time, table, data_type, flush=False):
    """

    :param symbols:
    :param begin_time:
    :param end_time:
    :param table:
    :param data_type:
    :param flush:
    :return:
    """
    s = SQLWrapper()
    r = RedisWrapper(db=AthenaConfig.hermes_db_index)

    if flush:
        r.hermes_db_protected = False
        r.flush_db()

    rows = s.select_hist_data(
        symbols_list=symbols,
        begin_time=begin_time,
        end_time=end_time,
        table=table
    )

    num_rows = len(rows)

    # transport k-lines
    if data_type == 'kl':
        counter = 0
        for row in rows:
            # compress to dictionary
            d_sql = dict(zip(SKf.kline_headers, row))

            # update time, dt and ft
            if type(d_sql[SKf.update_time]) == str:

                update_time = d_sql[SKf.update_time][:-1]
                dt = datetime.strptime(
                    update_time, AthenaConfig.sql_storage_dt_format)
                ex_update_time = d_sql[SKf.ex_update_time][:-1]
                dt_ex = datetime.strptime(
                    ex_update_time, AthenaConfig.sql_storage_dt_format)

            elif type(d_sql[SKf.update_time]) == datetime:

                dt = d_sql[SKf.update_time]
                dt_ex = d_sql[SKf.ex_update_time]

            else: raise TypeError

            ft = dt_to_filetime(dt)
            ft_ex = dt_to_filetime(dt_ex)

            contract = d_sql[SKf.contract]
            duration = int(d_sql[SKf.duration])
            duration_specifier = \
                AthenaConfig.hermes_kl_seconds_to_dur[duration]

            # map to pub channel
            pub_channel = \
                AthenaConfig.hermes_kl_mapping[duration_specifier][contract]

            # map to key
            athena_unique_key = pub_channel + ':' + str(ft)

            d_pub = {
                HKf.duration: duration,
                HKf.exchange: d_sql[SKf.exchange],
                HKf.day: d_sql[SKf.day],
                HKf.ex_time: ft_ex,
                HKf.local_time: ft,
                HKf.open_time: 0,
                HKf.high_time: 0,
                HKf.low_time: 0,
                HKf.close_time: ft_ex,
                HKf.pre_close_price: 0,
                HKf.open_price: int(float(d_sql[SKf.open_price]) * 10000),
                HKf.high_price: int(float(d_sql[SKf.high_price]) * 10000),
                HKf.low_price: int(float(d_sql[SKf.low_price]) * 10000),
                HKf.close_price: int(float(d_sql[SKf.close_price]) * 10000),
                HKf.average_price: int(
                    float(d_sql[SKf.average_price]) * 10000),
                HKf.volume: int(d_sql[SKf.volume]),
                HKf.turnover: int(d_sql[SKf.turnover]),
                HKf.total_volume: int(d_sql[SKf.total_volume]),
                HKf.total_turnover: int(d_sql[SKf.total_turnover]),
                HKf.open_interest: int(d_sql[SKf.open_interest])
            }

            r.set_dict(
                key=athena_unique_key,
                data=d_pub
            )

            counter += 1
            if not counter % 10000:
                print('[Redis]: Transported {}/{}.'.format(
                    counter, num_rows))

    # transport k-lines
    elif data_type == 'md':
        counter = 0
        for row in rows:
            # compress to dictionary
            d_sql = dict(zip(STf.tick_headers, row))

            # update time, dt and ft
            if type(d_sql[STf.local_update_time]) == str:

                update_time = d_sql[STf.local_update_time][:-1]
                dt = datetime.strptime(
                    update_time, AthenaConfig.sql_storage_dt_format)

                ex_update_time = d_sql[STf.ex_update_time][:-1]
                dt_ex = datetime.strptime(
                    ex_update_time, AthenaConfig.sql_storage_dt_format)

            elif type(d_sql[STf.local_update_time]) == datetime:

                dt = d_sql[STf.local_update_time]
                dt_ex = d_sql[STf.ex_update_time]

            else: raise TypeError

            ft = dt_to_filetime(dt)
            ft_ex = dt_to_filetime(dt_ex)

            contract = d_sql[STf.contract]

            # map to pub channel
            pub_channel = \
                AthenaConfig.hermes_md_mapping[contract]

            # map to key
            athena_unique_key = pub_channel + ':' + str(ft)

            d_pub = {
                HTf.exchange: d_sql[STf.exchange],
                HTf.day: d_sql[STf.day],
                HTf.ex_time: ft_ex,
                HTf.local_time: ft,
                HTf.update_ms: 0,
                HTf.last_price: int(float(d_sql[STf.last_price]) * 10000),
                HTf.volume: int(d_sql[STf.volume]),
                HTf.turnover: int(d_sql[STf.turnover]),
                HTf.open_interest: int(d_sql[STf.open_int]),
                HTf.pre_open_interest: 0,
                HTf.pre_clear_price: 0,
                HTf.pre_close_price: 0,
                HTf.average_price: int(
                    float(d_sql[STf.average_price]) * 10000),
                HTf.open_price: 0,
                HTf.close_price: 0,
                HTf.clear_price: 0,
                HTf.high_price: int(float(d_sql[STf.highest_price]) * 10000),
                HTf.low_price: int(float(d_sql[STf.lowest_price]) * 10000),
                HTf.bid_1: int(float(d_sql[STf.bid_1]) * 10000),
                HTf.bid_2: int(float(d_sql[STf.bid_2]) * 10000),
                HTf.bid_3: int(float(d_sql[STf.bid_3]) * 10000),
                HTf.bid_4: int(float(d_sql[STf.bid_4]) * 10000),
                HTf.bid_5: int(float(d_sql[STf.bid_5]) * 10000),
                HTf.bid_6: int(float(d_sql[STf.bid_6]) * 10000),
                HTf.bid_7: int(float(d_sql[STf.bid_7]) * 10000),
                HTf.bid_8: int(float(d_sql[STf.bid_8]) * 10000),
                HTf.bid_9: int(float(d_sql[STf.bid_9]) * 10000),
                HTf.bid_10: int(float(d_sql[STf.bid_10]) * 10000),
                HTf.bid_vol_1: int(d_sql[STf.bid_vol_1]),
                HTf.bid_vol_2: int(d_sql[STf.bid_vol_2]),
                HTf.bid_vol_3: int(d_sql[STf.bid_vol_3]),
                HTf.bid_vol_4: int(d_sql[STf.bid_vol_4]),
                HTf.bid_vol_5: int(d_sql[STf.bid_vol_5]),
                HTf.bid_vol_6: int(d_sql[STf.bid_vol_6]),
                HTf.bid_vol_7: int(d_sql[STf.bid_vol_7]),
                HTf.bid_vol_8: int(d_sql[STf.bid_vol_8]),
                HTf.bid_vol_9: int(d_sql[STf.bid_vol_9]),
                HTf.bid_vol_10: int(d_sql[STf.bid_vol_10]),
                HTf.ask_1: int(float(d_sql[STf.ask_1] )* 10000),
                HTf.ask_2: int(float(d_sql[STf.ask_2]) * 10000),
                HTf.ask_3: int(float(d_sql[STf.ask_3]) * 10000),
                HTf.ask_4: int(float(d_sql[STf.ask_4]) * 10000),
                HTf.ask_5: int(float(d_sql[STf.ask_5]) * 10000),
                HTf.ask_6: int(float(d_sql[STf.ask_6]) * 10000),
                HTf.ask_7: int(float(d_sql[STf.ask_7]) * 10000),
                HTf.ask_8: int(float(d_sql[STf.ask_8]) * 10000),
                HTf.ask_9: int(float(d_sql[STf.ask_9]) * 10000),
                HTf.ask_10: int(float(d_sql[STf.ask_10]) * 10000),
                HTf.ask_vol_1: int(d_sql[STf.ask_vol_1]),
                HTf.ask_vol_2: int(d_sql[STf.ask_vol_2]),
                HTf.ask_vol_3: int(d_sql[STf.ask_vol_3]),
                HTf.ask_vol_4: int(d_sql[STf.ask_vol_4]),
                HTf.ask_vol_5: int(d_sql[STf.ask_vol_5]),
                HTf.ask_vol_6: int(d_sql[STf.ask_vol_6]),
                HTf.ask_vol_7: int(d_sql[STf.ask_vol_7]),
                HTf.ask_vol_8: int(d_sql[STf.ask_vol_8]),
                HTf.ask_vol_9: int(d_sql[STf.ask_vol_9]),
                HTf.ask_vol_10: int(d_sql[STf.ask_vol_10]),
            }

            r.set_dict(
                key=athena_unique_key,
                data=d_pub
            )

            counter += 1
            if not counter % 10000:
                print('[Redis]: Transported {}/{}.'.format(
                    counter, num_rows))


if __name__ == '__main__':
    # symbols, begin_time, end_time, table, data_type
    make_dump(
        symbols=['GC1612'],
        begin_time=datetime(2016, 2, 1),
        end_time=datetime(2016, 3, 5),
        table='GCZ6kl160301',
        data_type='kl',
        flush=True
    )

    make_dump(
        symbols=['GC1612'],
        begin_time=datetime(2016, 2, 1),
        end_time=datetime(2016, 3, 5),
        table='GCZ6md160301',
        data_type='md',
        flush=False
    )


