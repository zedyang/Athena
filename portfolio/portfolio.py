from __future__ import division

from Athena.portfolio.position import Position, PositionDirection

__author__ = 'zed'


class Portfolio(object):
    """
    Portfolio class. This class encapsulates positions on multiple tickers
    as well as maintaining cash account.
    It adds/modifies positions when asked to, and export portfolio state,
    that is, equity/cash level. It also calculates total PnL of the account.
    """
    def __init__(self, tickers, init_cash):
        """
        Constructor.
        :param tickers: the tickers that will be included in portfolio.
        :param init_cash: initial cash level.
        :return:
        """
        self.tickers = tickers
        self.init_cash = init_cash
        self.cash = init_cash  # current cash level.
        self.equity = self.cash  # current equity value.

        self.positions = {}  # position is a dict, {ticker_name: Position}

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
        :param market_prices: a dict of {ticker: mkt_price},
            the market snapshot of all tickers that are being tracked.
        :return:
        """
        self.__reset()
        for ticker in self.positions:
            p = self.positions[ticker]
            try:
                price = market_prices[ticker]
                p.update_market_value(price)
            except KeyError:
                print("Last close price of {} is not available.".format(ticker))
            # sum up PnLs of individual positions.
            self.unrealized_pnl += p.unrealized_pnl
            self.realized_pnl += p.realized_pnl

            # calculate cash effect of this position
            cash_earned_on_transactions = p.realized_pnl - p.unrealized_pnl
            self.cash += (cash_earned_on_transactions - p.cost)

            # calculate equity effect
            self.equity = self.equity + \
                          p.market_value - p.cost + cash_earned_on_transactions

    def __add_position(self, ticker, direction, quantity, price, commission,
                       market_prices):
        """
        add new position to portfolio
        """
        self.__reset()
        if ticker not in self.positions:
            # make new position
            position = Position(ticker, direction,
                                quantity, price, commission)
            self.positions[ticker] = position
            self.update(market_prices)
        else:
            print("{} is already in the position list.".format(ticker))

    def __modify_position(self, ticker, direction, quantity, price, commission,
                          market_prices):
        """
        Modify the position when ticker is already in the list.
        """
        self.__reset()
        if ticker in self.positions:
            self.positions[ticker].transact(
                    direction, quantity, price, commission
            )
            self.positions[ticker].update_market_value(market_prices[ticker])
            self.update(market_prices)
        else:
            print("{} is not in the position list.".format(ticker))

    def transact_position(self, ticker, direction, quantity, price, commission,
                          market_prices):
        """
        just a wrapper of __add and __modify position methods.
        :param ticker: as is suggested
        :param direction:
        :param quantity:
        :param price:
        :param commission:
        :param market_prices: a dict of {ticker: mkt_price},
            the market snapshot of all tickers that are being tracked.
        :return:
        """
        if ticker not in self.positions:
            self.__add_position(ticker, direction, quantity, price,
                                commission, market_prices)
        else:
            self.__modify_position(ticker, direction, quantity, price,
                                   commission, market_prices)

    def make_portfolio_status(self, market_prices):
        """
        export a dict containing current status of portfolio.
        :param market_prices: a dict of {ticker: mkt_price}
        :return: dict containing "cash", "equity" and market values of
            each instrument.
        """
        self.__reset()
        self.update(market_prices)
        status = {
            'cash': self.cash,
            'equity': self.equity
        }
        for ticker in self.positions:
            status[ticker] = self.positions[ticker].market_value
        return status