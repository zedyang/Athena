import unittest

from Athena.portfolio.portfolio import Portfolio, PositionDirection

__author__ = 'zed'


class TestPortfolioBasic(unittest.TestCase):
    """
    Test portfolios
    """
    def setUp(self):
        tickers = ['S1','S2']
        # initialize portfolio with cash $10,000
        self.my_portfolio = Portfolio(tickers, init_cash=10000)
        # the market prices series are listed below, tickers are S1 and S2.
        self.market_prices_1 = [
            {'S1': 10, 'S2': 10},
            {'S1': 11, 'S2': 10},
            {'S1': 9, 'S2': 10},
            {'S1': 11, 'S2': 10},
            {'S1': 10, 'S2': 8},
            {'S1': 12, 'S2': 10},
            {'S1': 11, 'S2': 11},
        ]

        self.md_data = [
            {'contract': 'S1', 'ask': 10, 'bid': 10, 'tag':'md'},
            {'contract': 'S1', 'ask': 11, 'bid': 11, 'tag':'md'},
            {'contract': 'S1', 'ask': 12, 'bid': 12, 'tag':'md'},
            {'contract': 'S1', 'ask': 13, 'bid': 13, 'tag':'md'}
        ]

    def test_my_portfolio_market_1(self):
        market_prices = self.market_prices_1
        # >> buy 100 shares S1 at $10, commission $1
        self.my_portfolio.transact_position('S1', PositionDirection.BOT,
                                            100, 10, 1, market_prices[0])
        s = self.my_portfolio.make_portfolio_status(market_prices[0])
        self.assertEqual(s['cash'], 8999)
        self.assertEqual(s['equity'], 9999)
        self.assertEqual(s['S1'], 1000)

        # >> S1 goes to $11, no transaction
        s = self.my_portfolio.make_portfolio_status(market_prices[1])
        self.assertEqual(s['equity'], 10099)
        self.assertEqual(s['S1'], 1100)

        # >> S1 goes to $9, buy 100 more shares, commission $1
        # now 200 shares of S1.
        self.my_portfolio.transact_position('S1', PositionDirection.BOT,
                                            100, 9, 1, market_prices[2])
        # >> S1 goes to $11 again
        # market value of S1 is 11*200 = 2200
        # remained cash is 10000-900-1000-commission=8098
        # total equity is 2200+8098
        s = self.my_portfolio.make_portfolio_status(market_prices[3])
        self.assertEqual(s['S1'], 2200)
        self.assertEqual(s['cash'], 8098)
        self.assertEqual(s['equity'], 8098+2200)

        # >> buy 200 shares of S2 at $8, commission $2
        # cost = 1602
        # remained cash = 8098-1602 = 6496
        self.my_portfolio.transact_position('S2', PositionDirection.BOT,
                                            200, 8, 2, market_prices[4])

        # price goes to (12, 10)
        # market value S1: 2400, S2: 2000
        # total equity is 4400+6496=10896
        s = self.my_portfolio.make_portfolio_status(market_prices[5])
        self.assertEqual(s['cash'], 6496)
        self.assertEqual(s['S1'], 2400)
        self.assertEqual(s['S2'], 2000)
        self.assertEqual(s['equity'], 6496+2400+2000)

        # >> sell 100 shares of S1 at (12,10), commission $1
        self.my_portfolio.transact_position('S1', PositionDirection.SLD,
                                            100, 12, 1, market_prices[5])
        # cash added = 1200-1 = 1199
        # cash account = 1199+6496 = 7695
        s = self.my_portfolio.make_portfolio_status(market_prices[5])
        self.assertEqual(s['cash'], 7695)

        # >> close all positions at (11,11), commission $1, $2
        # cash added = 1100+2200-3 = 3297
        # cash account = 7695+3297 = 10992
        # total (realized) PnL is 992
        self.my_portfolio.transact_position('S1', PositionDirection.SLD,
                                            100, 11, 1, market_prices[6])
        self.my_portfolio.transact_position('S2', PositionDirection.SLD,
                                            200, 11, 2, market_prices[6])
        s = self.my_portfolio.make_portfolio_status(market_prices[6])
        self.assertEqual(s['cash'], 10992)
        self.assertEqual(s['S1'], 0)
        self.assertEqual(s['S2'], 0)
        self.assertEqual(s['equity'], 10992)
        self.assertEqual(self.my_portfolio.realized_pnl, 992)


class TestPortfolioBorrowing(unittest.TestCase):
    """
    Test portfolio with leverage. (negative cash balance)
    """
    def setUp(self):
        tickers = ['S1','S2']
        # initialize portfolio with cash $10,000
        self.my_portfolio = Portfolio(tickers, init_cash=10000)
        # the market prices series are listed below, tickers are S1 and S2.
        self.market_prices_1 = [
            {'S1': 10, 'S2': 10},
            {'S1': 12, 'S2': 10},
            {'S1': 8, 'S2': 10},
        ]

    def test_my_portfolio_market_1(self):
        market_prices = self.market_prices_1
        # >> buy 2000 shares S1 at $10, commission $5
        self.my_portfolio.transact_position('S1', PositionDirection.BOT,
                                            2000, 10, 5, market_prices[0])
        s = self.my_portfolio.make_portfolio_status(market_prices[0])

        # leverage = 2*
        # S1 goes to $12
        s = self.my_portfolio.make_portfolio_status(market_prices[1])
        self.assertEqual(self.my_portfolio.realized_pnl, 3995)

        # S1 goes down to $8
        s = self.my_portfolio.make_portfolio_status(market_prices[2])
        self.assertEqual(self.my_portfolio.realized_pnl, -4005)


if __name__ == '__main__':
    unittest.main()