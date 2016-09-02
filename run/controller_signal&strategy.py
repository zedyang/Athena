# basic lib
import time
import numpy as np
import multiprocessing as mp
from pyqtgraph import QtGui
from itertools import groupby

# basic system lib
from Athena.data_handler.data_handler import HermesDataHandler
from Athena.graphics_items.athena_window_new \
    import AthenaMainWindowController
from Athena.graphics_items.athena_window_spread \
    import AthenaSpreadWindowController

# basic signal (subscribed by strategies)
from Athena.signals.donchian_channel import DonchianChannelBatch
from Athena.signals.spread import FutureSpotSpread
from Athena.signals.spread_stopwin import SpreadStopWin
from Athena.signals.moving_average import MovingAverageBatch
from Athena.signals.trailing_stop import TrailingStop
from Athena.signals.market_profile import MarketProfile
from Athena.signals.demo_market_vol import MarketVolBatch
from Athena.signals.demo_correlation import CorrelationBatch

# basic strategy
from Athena.strategies.cta_strategy_1 import CTAStrategy1
from Athena.strategies.cta_strategy_2 import CTAStrategy2
from Athena.strategies.spread_strategy import SpreadStrategy1
from Athena.signal_transform.transform_market_vol import TransformMarketVol

__author__ = 'Atom'
# Configuration in the block below.
# -------------------------------------------------------------------
mode = 'paper'   # (paper, replay), paper trade or replay (backtest)

strategy_batch = [
    {
        'type': 'signalmarketvol',
        'contract': 'GC1612',
        'params': (36, 48, 20, 0.0008, 0.4),
        'tick_size': 0.1,
        'kl_duration': '1m'
    },
    {
        'type': 'cta1',
        'contract': 'GC1612',
        'params': (36, 48, 20, 0.0008, 0.4),
        'tick_size': 0.1,
        'kl_duration': '1m'
    },
    {
        'type': 'cta2',
        'contract': 'GC1612',
        'params': (26, 30, 20, 3, 0.4),
        'tick_size': 0.1,
        'kl_duration': '1m'
    },
    {
        'type': 'cta2',
        'contract': 'Au(T+D)',
        'params': (26, 30, 20, 3, 0.4),
        'tick_size': 0.1,
        'kl_duration': '1m'
    },
    {
        'type': 'spread1',
        'pair': ('Au(T+D)', 'au1612'),
        'params': (0.12, np.nan, 0.1),
        'kl_duration': '1m'
    }
]

global_config = {
    'market_profile': {
        'value_area': 0.7,
        'cycle': 30
    },
    'spread': {
        'cycle': 30
    }
}

# -------------------------------------------------------------------
#
# don't modify the code below.
#
# -------------------------------------------------------------------
contract_list = sorted([
    (b['contract'], b['kl_duration'], b['tick_size'])
    for b in strategy_batch if 'contract' in b
])
pair_list = sorted(
    (b['pair'], b['kl_duration'])
    for b in strategy_batch if 'pair' in b
)

contract_counts = [
    len(list(group)) for k, group in groupby(sorted(contract_list))
]

contract_set = sorted(list(set(contract_list)))
pair_set = sorted(list(set(pair_list)))

contract_counts_dict = dict(zip(
    [t[0] for t in contract_set], contract_counts
))

signal_config = {
    'ma': dict(),
    'don': dict(),
    'spread': dict(),
}
for tup in contract_list:
    signal_config['ma']['.'.join([tup[0], tup[1]])] = set([])
    signal_config['don']['.'.join([tup[0], tup[1]])] = set([])

for b in strategy_batch:
    if 'contract' in b:     # single instrument
        name = '.'.join((b['contract'], b['kl_duration']))
        if b['type'] == 'cta1':
            signal_config['ma'][name].add(b['params'][0])
            signal_config['ma'][name].add(b['params'][1])
            signal_config['don'][name].add(b['params'][2])
        elif b['type'] == 'cta2':
            signal_config['ma'][name].add(b['params'][0])
            signal_config['don'][name].add(b['params'][1])
            signal_config['don'][name].add(b['params'][2])
        elif b['type'] == 'signalmarketvol':
            pass


def sub_ma(c):
    suffix = '.'.join([c[0], c[1]])
    my_signal = MovingAverageBatch(
        [
            'kl:{}'.format(suffix)
        ],
        param_list=[list(signal_config['ma'][suffix]), 'close'],
        duplicate=contract_counts_dict[c[0]]
    )
    my_signal.start()


def sub_don(c):
    suffix = '.'.join([c[0], c[1]])
    my_signal = DonchianChannelBatch(
        [
            'kl:{}'.format(suffix)
        ],
        param_list=[list(signal_config['don'][suffix])],
        duplicate=contract_counts_dict[c[0]]
    )
    my_signal.start()


def sub_mp(c):
    my_signal = MarketProfile(
        [
            'kl:{}'.format(
                '.'.join([c[0], c[1]])
            )
        ],
        param_list=[
            global_config['market_profile']['value_area'],
            global_config['market_profile']['cycle'],
            c[2]
        ],
        duplicate=contract_counts_dict[c[0]]
    )
    my_signal.start()


def sub_signal_marketvol(c):
    suffix = '.'.join([c[0], c[1]])
    my_signal = MarketVolBatch(
        [
            'kl:{}'.format(suffix)
        ],
        param_list=[[5, 4, 3], 'close'],
        duplicate=contract_counts_dict[c[0]]
    )
    my_signal.start()


def sub_correlation():
    signal_correlation = CorrelationBatch(
        [
            'kl:{}.1m'.format('GC1612'),
            'kl:{}.1m'.format('Au(T+D)')
        ],
        param_list=[('GC1612', 'Au(T+D)'),
                    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                    16,17,18,19,20,21,22,23,24,25,26,27,28,29,30],
                    [1, 2, 3, 4, 5, 6, 7, 8]]
    )
    signal_correlation.start()

    # 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
    # 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60]

def sub_spread(c):
    spread_signal = FutureSpotSpread(
        [
            'md:{}'.format(c[0][0]),
            'md:{}'.format(c[0][1]),
            'kl:{}.{}'.format(c[0][0], c[1]),
            'kl:{}.{}'.format(c[0][1], c[1])
        ],
        param_list=[
            c[0],
            global_config['spread']['cycle']
        ]
    )
    spread_signal.start()


def sub_stop(b):
    if b['type'] == 'spread1':
        pair_name = '.'.join(b['pair'])
        stop_signal = SpreadStopWin(
            [
                'strategy:spread_1.{}'.format(pair_name),
                'signal:spread.{}'.format(pair_name)
            ],
            param_list=[
                b['pair'],
                b['params'][2],
                'strategy:spread_1.{}'.format(pair_name),
                False
            ]
        )
        stop_signal.start()
    else:
        return


def sub_trailing_cta1(b):
    if b['type'] == 'cta1':
        trailing_stop = TrailingStop(
            [
                'kl:{}.{}'.format(
                    b['contract'],
                    b['kl_duration']
                ),
                'strategy:cta_1.{}'.format(b['contract'])
            ],
            param_list=[
                b['contract'],
                'strategy:cta_1.{}'.format(b['contract'])]
        )
        trailing_stop.start()
    else:
        return


def sub_strategy(b):
    if 'contract' in b:
        kl = '.'.join([b['contract'], b['kl_duration']])
        if b['type'] == 'cta1':
            my_strategy = CTAStrategy1(
                [
                    'md:{}'.format(b['contract']),
                    'kl:{}'.format(kl),
                    'signal:donchian.{}'.format(kl),
                    'signal:ma.{}'.format(kl),
                    'signal:trailing.stop.{}'.format(b['contract'])
                ],
                param_list=b['params'],
                instrument=b['contract']
            )
            my_strategy.start()

        elif b['type'] == 'cta2':
            my_strategy = CTAStrategy2(
                [
                    'md:{}'.format(b['contract']),
                    'kl:{}'.format(kl),
                    'signal:donchian.{}'.format(kl),
                    'signal:ma.{}'.format(kl),
                ],
                param_list=b['params'],
                instrument=b['contract']
            )
            my_strategy.start()

        elif b['type'] == 'signalmarketvol':
            my_strategy = TransformMarketVol(
                [
                    'md:{}'.format(b['contract']),
                    'kl:{}'.format(kl),
                    'signal:market_vol.{}.1m'.format(b['contract'])
                ],
                param_list=b['params'],
                instrument=b['contract']
            )
            my_strategy.start()

    elif 'pair' in b:
        pair_name = '.'.join(b['pair'])
        if b['type'] == 'spread1':
            my_strategy = SpreadStrategy1(
                subscribe_list=[
                    'signal:spread.{}'.format(pair_name),
                    'signal:spread.stop.{}'.format(pair_name)
                ],
                param_list=b['params'],
                pair=b['pair']
            )
            my_strategy.start()


def window_single(batches):
    athena = AthenaMainWindowController()
    for b in batches:
        if 'contract' in b:
            if contract_counts_dict[b['contract']] == 1:
                index = -1
            else:
                if b['type'] == 'cta1':
                    index = 0
                elif b['type'] == 'cta2':
                    index = 1
                elif b['type'] == 'signalmarketvol':
                    index = 2
                else:
                    index = -1

            if b['type'] == 'cta1':
                athena.add_cta1_instance(
                    instrument=b['contract'],
                    params_list=b['params'],
                    plot_channel_index=index,
                    duration_specifier=b['kl_duration'],
                )
            elif b['type'] == 'cta2':
                athena.add_cta2_instance(
                    instrument=b['contract'],
                    params_list=b['params'],
                    plot_channel_index=index,
                    duration_specifier=b['kl_duration'],
                )
            elif b['type'] == 'signalmarketvol':
                athena.add_signal_marketvol(
                    instrument=b['contract'],
                    params_list=b['params'],
                    plot_channel_index=index,
                    duration_specifier=b['kl_duration'],
                )
    QtGui.QApplication.instance().exec_()


def window_pair(batches):
    athena = AthenaSpreadWindowController()
    for b in batches:
        if 'pair' in b:
            if b['type'] == 'spread1':
                athena.add_spread1_instance(
                    pair=b['pair'],
                    param_list=b['params']
                )
    QtGui.QApplication.instance().exec_()


def hermes_pub():
    contracts = list(set([b[0] for b in contract_list]))
    data_handler = HermesDataHandler()
    for c in contracts:
        data_handler.add_instrument(c, duplicate=contract_counts_dict[c],
                                    kline_dur_specifiers=('1m',))
    if mode == 'replay':
        data_handler.replay_data(attach_end_flag=True)
    elif mode == 'paper':
        data_handler.distribute_data()


if __name__ == '__main__':
    mp.set_start_method('spawn')

    # hermes driver process
    p_pub = mp.Process(target=hermes_pub)

    # qt window process
    p_window = mp.Process(target=window_single, args=(strategy_batch,))
    p_window_2 = mp.Process(target=window_pair, args=(strategy_batch,))

    sub_process_pool = []
    # signal sub processes
    for c_tup in contract_set:
        p_ma = mp.Process(target=sub_ma, args=(c_tup,))
        p_don = mp.Process(target=sub_don, args=(c_tup,))
        p_mp = mp.Process(target=sub_mp, args=(c_tup,))
        p_marketvol = mp.Process(target=sub_signal_marketvol, args=(c_tup,))
        sub_process_pool.extend([p_ma, p_don, p_mp, p_marketvol])

    # invided signal
    p_correlation = mp.Process(target=sub_correlation, )
    sub_process_pool.extend([p_correlation])

    for p_tup in pair_set:
        p_spread = mp.Process(target=sub_spread, args=(p_tup,))
        sub_process_pool.extend([p_spread])

    # strategy processes
    for b in strategy_batch:
        p_trailing = mp.Process(target=sub_trailing_cta1, args=(b,))
        p_stop = mp.Process(target=sub_stop, args=(b,))
        p_strategy = mp.Process(target=sub_strategy, args=(b,))
        sub_process_pool.extend([p_trailing, p_stop, p_strategy])

    time.sleep(1)
    p_window.start()
    time.sleep(1)
    p_window_2.start()
    print('[Athena]: Spawning processes...')
    time.sleep(1)
    [p.start() for p in sub_process_pool]
    time.sleep(1)
    p_pub.start()
