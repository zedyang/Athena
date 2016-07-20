import json

from Athena.settings import AthenaConfig, AthenaProperNames
from Athena.utils import append_digits_suffix_for_redis_key
from Athena.portfolio.position import Position, PositionDirection
from Athena.data_handler.redis_wrapper import RedisWrapper

__author__ = 'zed'


class Portfolio(object):
    """
    Portfolio class. This class encapsulates positions on multiple tickers
    as well as maintaining cash account.
    It adds/modifies positions when asked to, and export portfolio state,
    that is, equity/cash level. It also calculates total PnL of the account.
    """
    def __init__(self, instruments_list, init_cash, strategy_channels=None):
        """
        Constructor.
        :param instruments_list: the tickers that will be included
        in portfolio.
        :param init_cash: initial cash level.
        :param strategy_channels: buy/sell signal channels to subscribe.
        :return:
        """
        self.instruments_list = instruments_list
        self.strategy_channels = strategy_channels
        self.pub_channel = 'portfolio'

        self.init_cash = init_cash
        self.cash = init_cash  # current cash level.
        self.equity = self.cash  # current equity value.

        # position is a dict, {instrument_name: Position}
        self.positions = dict()
        self.market_prices = dict()
        for instrument in self.instruments_list:
            self.market_prices[instrument] = None

        self.realized_pnl = 0
        self.unrealized_pnl = 0

        # open connection and subscribe
        self.counter = 0
        self.__subscribe()

    def __subscribe(self):
        """
        - open two connections to redis. One is to listen to market data,
        and buy/sell signals. Another is to write portfolio information.
        - subscribe some instrument by listening to the channel in Redis.
        :return:
        """
        # open two connections
        self.sub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)
        self.pub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)

        # create a sub.
        self.sub = self.sub_wrapper.connection.pubsub()

        # channel directory (set in MarketDataHandler
        sub_instruments_list = ['md:' + inst for inst in self.instruments_list]
        self.sub.subscribe(sub_instruments_list)
        self.sub.subscribe(self.strategy_channels)

    def on_message(self, message):
        """

        :return:
        """
        if message['type'] == AthenaProperNames.md_message_type:
            # on md messages
            this_instrument = message[AthenaConfig.sql_instrument_field]
            self.market_prices[this_instrument] = (
                (float(message[AthenaConfig.sql_ask_field]) +
                 float(message[AthenaConfig.sql_bid_field])) / 2
            )
        if message['type'] == AthenaProperNames.order_message_type:
            # on buy/sell signal messages
            self.transact_position(
                instrument=message['contract'],
                direction=(
                    PositionDirection.BOT
                    if message['direction'] == AthenaProperNames.long
                    else PositionDirection.SLD
                ),
                quantity=message['quantity'],
                price=message['price'],
                commission=message['commission'],
                market_prices=self.market_prices
            )

        # publish message
        status = self.make_portfolio_status(self.market_prices)
        status['update_time'] = message['update_time'] \
            if message['type'] == 'order' \
            else message[AthenaConfig.sql_local_dt_field]
        status['event_type'] = 'update' \
            if message['type'] == AthenaProperNames.md_message_type \
            else 'trade'
        self.publish(status)

        # increment to counter
        self.counter += 1

    def start(self):
        """
        let portfolio start listening.
        """
        for message in self.sub.listen():
            if message['type'] == 'message':
                str_data = message['data'].decode('utf-8')
                dict_data = json.loads(str_data)
                self.on_message(list(dict_data.values())[0])

    def publish(self, dict_message_data):
        """
        set hash table in redis and publish message in its own channel.
        The name of channel is just self.signal_name
        :param dict_message_data: dictionary, the data to be published
        :return:
        """
        # create hash set object in redis.
        dict_message_data['type'] = AthenaProperNames.portfolio_message_type

        published_key = append_digits_suffix_for_redis_key(
            prefix='portfolio',
            counter=self.counter,
        )
        self.pub_wrapper.set_dict(published_key, dict_message_data)

        # serialize json dict to string.
        published_message = json.dumps({published_key: dict_message_data})

        # publish the message to support other subscriber.
        self.pub_wrapper.connection.publish(
            channel=self.pub_channel,
            message=published_message
        )

    def __reset(self):
        """
        reset all variables before calculation
        :return:
        """
        self.cash = self.init_cash
        self.equity = self.cash
        self.realized_pnl = 0
        self.unrealized_pnl = 0

    def update(self, market_prices):
        """
        update portfolio on market prices.
        :param market_prices: a dict of {instrument: mkt_price},
            the market snapshot of all tickers that are being tracked.
        :return:
        """
        self.__reset()
        for instrument in self.positions:
            p = self.positions[instrument]
            try:
                price = market_prices[instrument]
                p.update_market_value(price)
            except KeyError:
                print("Last close price of {} is not available.".format(
                    instrument))
            # sum up PnLs of individual positions.
            self.unrealized_pnl += p.unrealized_pnl
            self.realized_pnl += p.realized_pnl

            # calculate cash effect of this position
            cash_earned_on_transactions = p.realized_pnl - p.unrealized_pnl
            self.cash += (cash_earned_on_transactions - p.cost)

            # calculate equity effect
            self.equity = self.equity + \
                          p.market_value - p.cost + cash_earned_on_transactions

    def __add_position(self, instrument, direction, quantity, price,
                       commission, market_prices):
        """
        add new position to portfolio
        """
        self.__reset()
        if instrument not in self.positions:
            # make new position
            position = Position(instrument, direction,
                                quantity, price, commission)
            self.positions[instrument] = position
            self.update(market_prices)
        else:
            print("{} is already in the position list.".format(instrument))

    def __modify_position(self, instrument, direction, quantity, price,
                          commission, market_prices):
        """
        Modify the position when instrument is already in the list.
        """
        self.__reset()
        if instrument in self.positions:
            self.positions[instrument].transact(
                    direction, quantity, price, commission
            )
            self.positions[instrument].update_market_value(
                market_prices[instrument])
            self.update(market_prices)
        else:
            print("{} is not in the position list.".format(instrument))

    def transact_position(self, instrument, direction, quantity, price,
                          commission, market_prices):
        """
        just a wrapper of __add and __modify position methods.
        :param instrument: as is suggested
        :param direction:
        :param quantity:
        :param price:
        :param commission:
        :param market_prices: a dict of {instrument: mkt_price},
            the market snapshot of all tickers that are being tracked.
        :return:
        """
        if instrument not in self.positions:
            self.__add_position(instrument, direction, quantity, price,
                                commission, market_prices)
        else:
            self.__modify_position(instrument, direction, quantity, price,
                                   commission, market_prices)

    def make_portfolio_status(self, market_prices):
        """
        export a dict containing current status of portfolio.
        :param market_prices: a dict of {instrument: mkt_price}
        :return: dict containing "cash", "equity" and market values of
            each instrument.
        """
        self.__reset()
        self.update(market_prices)
        status = {
            'cash': self.cash,
            'equity': self.equity
        }
        for instrument in self.positions:
            status[instrument] = self.positions[instrument].market_value
        return status
