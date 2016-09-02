import json
import numpy as np

from Athena.settings import AthenaConfig
from Athena.data_handler.redis_wrapper import RedisWrapper
from Athena.portfolio.portfolio import Portfolio, PositionDirection
Tf, Kf, Of = AthenaConfig.HermesTickFields, AthenaConfig.HermesKLineFields, \
             AthenaConfig.OrderFields

__author__ = 'zed'


class ParametricNode(object):
    """

    """
    def __init__(self,
                 traded_instruments,
                 strategy_name_prefix,
                 param_names,
                 param_list):
        """

        :param traded_instruments:
        :param strategy_name_prefix:
        :param param_names:
        :param param_list:
        """
        # compress parameters
        self.dimension = len(param_names)
        self.param_list = param_list
        self.param_dict = dict(zip(param_names, param_list))

        # portfolio
        self.instruments_list = traded_instruments
        self.portfolio = Portfolio(traded_instruments)
        self.init_equity = self.portfolio.equity
        self.current_equity = self.init_equity

        # open connection
        self.redis_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)
        self.sub = self.redis_wrapper.connection.pubsub()

        # subscribe
        self.sub_channel \
            = strategy_name_prefix + '.' + str(param_list)
        self.strategy_full_name = self.sub_channel
        self.sub.subscribe(self.sub_channel)
        self.sub.subscribe('flags')

    def start(self):
        """

        """
        for message in self.sub.listen():
            if message['type'] == 'message':
                str_data = message['data'].decode('utf-8')
                dict_data = json.loads(str_data)
                d = list(dict_data.values())[0]

                # operations on flags
                if d['tag'] == 'flag':
                    if d['type'] == 'flag_0':
                        self.__publish_result()
                        return
                else:
                    self.on_message(d)

    def __publish_result(self):
        """

        :return:
        """
        hist_positions = []
        for inst in self.instruments_list:
            hist_positions.extend(self.portfolio.historical_positions[inst])

        num_positions = len(hist_positions)
        longs = [d for d in hist_positions
                 if d['direction'] == PositionDirection.BOT]
        num_long_positions = len(longs)

        wins = [d for d in hist_positions if d['realized_pnl'] > 0]
        num_wins = len(wins)
        wins_ratio = num_wins / num_positions if num_positions > 0 else 0

        holding_times = [d['holding_period'] for d in hist_positions]
        avg_holding_time = np.mean(holding_times)

        athena_unique_key = \
            'parametric_space:node.' + str(self.param_list)

        to_publish = {
            'init_equity': self.init_equity,
            'final_equity': self.current_equity,
            'payoff': self.current_equity-self.init_equity,
            'num_position': num_positions,
            'num_long': num_long_positions,
            'winning_ratio': wins_ratio,
            'avg_holding_period': avg_holding_time,
        }

        for k in self.param_dict:
            to_publish[k] = self.param_dict[k]

        # publish
        self.redis_wrapper.set_dict(
            athena_unique_key, to_publish
        )

    def on_message(self, message):
        """

        :param message:
        :return:
        """
        if message['tag'] == self.strategy_full_name:

            # get market snapshot
            market_prices = dict()
            for inst in self.instruments_list:

                latest_tick = \
                    self.redis_wrapper.get_dict('md:{}:0'.format(inst))
                if latest_tick:
                    market_prices[inst] = float(latest_tick[Tf.last_price])

            # transact position
            if message[Of.direction] in ['long', 'short'] and market_prices:

                self.portfolio.transact_position(
                    instrument=message[Of.contract],
                    transaction_time=message[Of.bar_count],
                    direction=(
                        PositionDirection.BOT
                        if message[Of.direction] == 'long'
                        else PositionDirection.SLD
                    ),
                    quantity=message[Of.quantity],
                    price=message[Of.price],
                    commission=message[Of.commission],
                    market_prices=market_prices
                )

                self.current_equity = self.portfolio.equity

