import time
import numpy as np
import multiprocessing as mp
from pyqtgraph import QtGui

from Athena.data_handler.data_handler import HermesDataHandler
from Athena.data_handler.redis_wrapper import RedisWrapper
from Athena.signals.spread import FutureSpotSpread
from Athena.signals.spread_stopwin import SpreadStopWin
from Athena.strategies.spread_strategy import SpreadStrategy1
from Athena.optimizer.parametric_space import ParametricNode
from Athena.settings import AthenaConfig

__author__ = 'zed'

width_range = [i*0.01 for i in range(1, 15)]
stop_range = [j*0.01 for j in range(1, 15)]
all_params = [(a, np.nan, c) for a in width_range for c in stop_range]

chunk_size = 10
param_batches = [all_params[i:i+chunk_size]
                 for i in range(0, len(all_params), chunk_size)]


def sub_spread(pair):
    spread_signal = FutureSpotSpread(
        [
            'md:{}'.format(pair[0]),
            'md:{}'.format(pair[1]),
            'kl:{}.1m'.format(pair[0]),
            'kl:{}.1m'.format(pair[1])
        ],
        param_list=[pair, 30]
    )
    spread_signal.start()


def sub_stop(pair, params):
    stop_signal = SpreadStopWin(
        [
            'strategy:spread_1.({}, {}, {})'.format(
                params[0], params[1], params[2]),
            'signal:spread.{}.{}'.format(
                pair[0], pair[1])
        ],
        param_list=[
            pair,
            params[2],
            'strategy:spread_1.({}, {}, {})'.format(
                params[0], params[1], params[2]),
            True
        ]
    )
    stop_signal.start()


def sub_strategy(pair, params):
    my_strategy = SpreadStrategy1(
        subscribe_list=[
            'signal:spread.{}.{}'.format(
                pair[0], pair[1]),
            'signal:spread.stop.{}.{}.({}, {}, {})'.format(
                pair[0], pair[1], params[0], params[1], params[2])
        ],
        param_list=params,
        pair=pair
    )
    my_strategy.start()


def sub_node(pair, params):
    node = ParametricNode(
        traded_instruments=pair,
        strategy_name_prefix=SpreadStrategy1.strategy_name_prefix,
        param_names=SpreadStrategy1.param_names,
        param_list=params
    )
    node.start()


def hermes_pub():
    data_handler = HermesDataHandler()
    data_handler.add_instrument('au1612', kline_dur_specifiers=('1m',))
    data_handler.add_instrument('Au(T+D)', kline_dur_specifiers=('1m',))
    data_handler.replay_data(attach_end_flag=True)


if __name__ == '__main__':
    mp.set_start_method('spawn')
    spread_legs = ['Au(T+D)', 'au1612']
    r = RedisWrapper(db=AthenaConfig.athena_db_index)

    counter = 0
    for pb in param_batches:
        p_spread = mp.Process(target=sub_spread, args=(spread_legs,))
        processes = [p_spread]

        for p in pb:
            p1 = mp.Process(target=sub_strategy, args=(spread_legs, p,))
            p2 = mp.Process(target=sub_stop, args=(spread_legs, p,))
            p3 = mp.Process(target=sub_node, args=(spread_legs, p,))
            processes.extend([p1, p2, p3])

        p_pub = mp.Process(target=hermes_pub)

        time.sleep(1)
        [p.start() for p in processes]
        time.sleep(1)
        p_pub.start()

        counter += 1

        processes[-1].join()

        kk = r.get_keys('parametric_space:*')
        r.migrate_keys(kk, target_db=2)
        print('Finished chunk {}.'.format(counter))
