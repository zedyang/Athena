import json

from Athena.settings import AthenaConfig, AthenaProperNames
from Athena.utils import append_digits_suffix_for_redis_key
from Athena.portfolio.position import Position, PositionDirection
from Athena.db_wrappers.redis_wrapper import RedisWrapper
Tf = AthenaConfig.TickFields

__author__ = 'zed'


class Portfolio(object):
    """
    Portfolio class. This class encapsulates positions on multiple tickers
    as well as maintaining cash account.
    It adds/modifies positions when asked to, and export portfolio state,
    that is, equity/cash level. It also calculates total PnL of the account.
    """
    def __init__(self, instruments_list, init_cash=10000):
        """
        Constructor.
        :param instruments_list: the tickers that will be included
        in portfolio.
        :param init_cash: initial cash level.
        :return:
        """
        self.instruments_list = instruments_list

        self.init_cash = init_cash
        self.cash = init_cash  # current cash level.
        self.equity = self.cash  # current equity value.

        # position is a dict, {instrument_name: Position}
        self.positions = dict()
        self.market_prices = dict()
        for instrument in self.instruments_list:
            self.market_prices[instrument] = None

        # historical records of transactions
        self.historical_positions = dict()
        for instrument in self.instruments_list:
            self.historical_positions[instrument] = []

        self.realized_pnl = 0
        self.unrealized_pnl = 0

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
            p_hist = self.historical_positions[instrument]
            try:
                price = market_prices[instrument]
                p.update_market_value(price)
            except KeyError:
                print("Last close price of {} is not available.".format(
                    instrument))

            # single instrument
            p_realized_pnl = p.realized_pnl + sum(
                [ph['realized_pnl'] for ph in p_hist])
            p_unrealized_pnl = p.realized_pnl

            # sum up PnLs of individual positions.
            self.unrealized_pnl += p_unrealized_pnl
            self.realized_pnl += p_realized_pnl

            # calculate cash effect of this position
            cash_earned_on_transactions = p_realized_pnl - p_unrealized_pnl
            self.cash += (cash_earned_on_transactions - p.cost)

            # calculate equity effect
            self.equity = self.equity + \
                          p.market_value - p.cost + cash_earned_on_transactions

    def __add_position(self, instrument, transaction_time,
                       direction, quantity, price,
                       commission, market_prices):
        """
        add new position to portfolio
        """
        self.__reset()
        if instrument not in self.positions:
            # make new position
            position = Position(instrument, transaction_time, direction,
                                quantity, price, commission)
            self.positions[instrument] = position
            self.update(market_prices)
        else:
            print("{} is already in the position list.".format(instrument))

    def __modify_position(self, instrument, transaction_time,
                          direction, quantity, price,
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

            # if position quantity is 0, close the position
            # and add to history
            if self.positions[instrument].quantity == 0:
                this_position = self.positions[instrument]
                self.historical_positions[instrument].append(
                    {
                        'direction': this_position.direction,
                        'realized_pnl': this_position.realized_pnl,
                        'contract': instrument,
                        'holding_period': (
                            transaction_time - this_position.open_time
                        ),
                        'total_buy_volume': this_position.bought
                    }
                )
                del self.positions[instrument]
        else:
            print("{} is not in the position list.".format(instrument))

    def transact_position(self, instrument, transaction_time,
                          direction, quantity, price,
                          commission, market_prices):
        """
        just a wrapper of __add and __modify position methods.
        :param instrument: as is suggested
        :param transaction_time
        :param direction:
        :param quantity:
        :param price:
        :param commission:
        :param market_prices: a dict of {instrument: mkt_price},
            the market snapshot of all tickers that are being tracked.
        :return:
        """
        if instrument not in self.positions:
            self.__add_position(
                instrument,
                transaction_time,
                direction,
                quantity,
                price,
                commission,
                market_prices
            )
        else:
            self.__modify_position(
                instrument,
                transaction_time,
                direction,
                quantity, price,
                commission,
                market_prices
            )

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
