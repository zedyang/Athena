import time
import multiprocessing as mp
from pyqtgraph import QtGui

from Athena.signals.donchian_channel import DonchianChannelBatch
from Athena.signals.moving_average import MovingAverageBatch
from Athena.optimizer.parametric_space import ParametricNode
from Athena.data_handler.redis_wrapper import RedisWrapper
from Athena.strategies.cta_strategy_2 import CTAStrategy2
from Athena.data_handler.data_handler import HermesDataHandler
from Athena.settings import AthenaConfig

__author__ = 'zed'

# cta_2 parameter list:
# ('ma', 'don_up', 'don_down', 'stop_win', 'trailing')
contract = 'GC1612'
stop_range = [j for j in range(1, 3)]  # close at the left hand open
                                       # at the right hand
ma_range = [10*m for m in range(2, 6)]
don_range = [10*d for d in range(2, 4)]

all_params = [(m, d, d, 1, 1)
              for m in ma_range for d in don_range]

chunk_size = 12

param_batches = [all_params[i:i+chunk_size]
                 for i in range(0, len(all_params), chunk_size)]


def sub_ma(instrument):
    my_signal = MovingAverageBatch(
        [
            'kl:{}.1m'.format(instrument)
        ],
        param_list=[ma_range, 'close']
    )
    my_signal.start()


def sub_don(instrument):
    my_signal = DonchianChannelBatch(
        [
            'kl:{}.1m'.format(instrument)
        ],
        param_list=[don_range]
    )
    my_signal.start()


def sub_strategy(instrument, param):
    my_strategy = CTAStrategy2(
        [
            'md:{}'.format(instrument),
            'kl:{}.1m'.format(instrument),
            'signal:donchian.{}.1m'.format(instrument),
            'signal:ma.{}.1m'.format(instrument),
        ],
        param_list=param,
        instrument=instrument,
        train=1
    )
    my_strategy.start()


def sub_node(instrument, param):
    node = ParametricNode(
        traded_instruments=[instrument],
        strategy_name_prefix=CTAStrategy2.strategy_name_prefix+'.'+instrument,
        param_names=CTAStrategy2.param_names,
        param_list=param
    )
    node.start()


def hermes_pub():
    data_handler = HermesDataHandler()
    data_handler.add_instrument(contract, kline_dur_specifiers=('1m',))
    data_handler.replay_data(attach_end_flag=True)
    # data_handler.replay_data(attach_end_flag=False)


if __name__ == '__main__':
    mp.set_start_method('spawn')

    r = RedisWrapper(db=AthenaConfig.athena_db_index)
    r2 = RedisWrapper(db=2)
    r2.flush_db()

    counter = 0
    inst = contract
    for pb in param_batches:
        p_ma = mp.Process(target=sub_ma, args=(inst,))
        p_don = mp.Process(target=sub_don, args=(inst,))
        processes = [p_ma, p_don]

        for p in pb:
            p1 = mp.Process(target=sub_strategy, args=(inst,p,))
            p2 = mp.Process(target=sub_node, args=(inst,p,))
            processes.extend([p1, p2])

        p_pub = mp.Process(target=hermes_pub)

        time.sleep(1)
        [p.start() for p in processes]

        time.sleep(1)
        p_pub.start()

        counter += 1
        processes[-1].join()

        kk = r.get_keys('parametric_space:*')
        print(kk)
        r.migrate_keys(kk, target_db=2)
        print(r)
        print('Finished chunk {}.'.format(counter))
