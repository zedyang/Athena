from __future__ import division
import unittest

from Athena.portfolio.position import Position, PositionDirection

__author__ = 'zed'


class TestPosition(unittest.TestCase):
    """
    Test positions.
    """
    def setUp(self):
        """
        :return:
        """
        self.pos_long = Position('IC', PositionDirection.BOT, 100, 10, 1)
        self.pos_short = Position('IF', PositionDirection.SLD, 200, 5, 1)

    def test_long_position_schedule(self):
        # >>> open long position, $10 * 100 shares, commission $1
        self.assertEqual(self.pos_long.bought, 100)
        self.assertEqual(self.pos_long.sold, 0)
        self.assertEqual(self.pos_long.avg_bought_price, 10)
        # total cost should be 1000 + commission
        self.assertEqual(self.pos_long.cost, 1001)

        # >>> market price goes to 20, no transaction
        self.pos_long.update_market_value(20)
        self.assertEqual(self.pos_long.unrealized_pnl, 999)

        # >>> market price goes to 5, no transaction
        self.pos_long.update_market_value(5)
        self.assertEqual(self.pos_long.unrealized_pnl, -501)

        # >>> long 100 more shares at $8, commission $1
        # 200 shares total, avg_bought_price is 9.
        self.pos_long.transact(PositionDirection.BOT, 100, 8, 1)
        self.pos_long.update_market_value(8)
        self.assertEqual(self.pos_long.bought, 200)
        self.assertEqual(self.pos_long.avg_bought_price, 9)
        self.assertEqual(self.pos_long.total_bought, 1800)
        self.assertEqual(self.pos_long.total_commission, 2)
        # avg_price including commission should be (1001 + 801) / 200 = 9.01
        self.assertEqual(self.pos_long.avg_price, 9.01)
        self.assertEqual(self.pos_long.cost, 1802)

        # >>> sell 100 shares at $12, commission $1.5
        self.pos_long.transact(PositionDirection.SLD, 100, 12, 1.5)
        self.pos_long.update_market_value(12)
        # should remain 100 shares now
        self.assertEqual(self.pos_long.quantity, 100)
        self.assertEqual(self.pos_long.sold, 100)
        self.assertEqual(self.pos_long.avg_sold_price, 12)
        # unrealized pnl should be (12-9.01)*100 = 299
        self.assertEqual(self.pos_long.unrealized_pnl, 299)
        # if close position at $12, realized pnl should be 2400-1800-3.5
        self.assertEqual(self.pos_long.realized_pnl, 596.5)
        # the cash earned on this transaction is (12-9.01)*100-1.5=297.5
        # which is actually 596.5 - 299
        # cash_earned_on_transaction = realized_pnl - unrealized_pnl

        # >>> sell 100 shares at $10, commission $1
        self.pos_long.transact(PositionDirection.SLD, 100, 10, 1)
        self.pos_long.update_market_value(10)
        # remaining 0 share, the position is closed.
        # realized pnl is 1000+1200-1000-800-commission
        # total bought is 200, money amount is 1800
        # total sold is 200, money amount is 2200
        # avg_sold_price is 2200/200 = 11
        self.assertEqual(self.pos_long.quantity, 0)
        self.assertEqual(self.pos_long.net, 0)
        self.assertEqual(self.pos_long.bought, 200)
        self.assertEqual(self.pos_long.sold, 200)
        self.assertEqual(self.pos_long.total_bought, 1800)
        self.assertEqual(self.pos_long.total_sold, 2200)
        self.assertEqual(self.pos_long.unrealized_pnl, 0)
        self.assertEqual(self.pos_long.avg_sold_price, 11)
        self.assertEqual(self.pos_long.total_commission, 4.5)
        self.assertEqual(self.pos_long.realized_pnl, 2200-1800-4.5)

    def test_short_position_schedule(self):
        # >>> opened short position, $5 * 200 shares, commission $1
        self.assertEqual(self.pos_short.bought, 0)
        self.assertEqual(self.pos_short.sold, 200)
        self.assertEqual(self.pos_short.quantity, -200)
        self.assertEqual(self.pos_short.avg_sold_price, 5)
        # total cost should be negative (cash inflow)
        # which is -(1000-1) = -999
        self.assertEqual(self.pos_short.cost, -999)

        # >>> market price goes to 3, no transaction
        # -200*(3-5)-commission = 400 - 1 = 399
        self.pos_short.update_market_value(3)
        self.assertEqual(self.pos_short.unrealized_pnl, 399)

        # >>> market price goes to 6, no transaction
        self.pos_short.update_market_value(6)
        self.assertEqual(self.pos_short.unrealized_pnl, -201)

        # >>> short 100 more shares at $8, commission $2
        # now 300 shares, avg_sold_price is (800+1000)/300 = 6
        self.pos_short.transact(PositionDirection.SLD, 100, 8, 2)
        self.pos_short.update_market_value(8)
        self.assertEqual(self.pos_short.sold, 300)
        self.assertEqual(self.pos_short.avg_sold_price, 6)
        self.assertEqual(self.pos_short.total_sold, 1800)
        self.assertEqual(self.pos_short.total_commission, 3)
        # total cash inflow is 1800-commission = 1797 (variable cost)
        # avg_price is 1797/300 = 5.99
        self.assertEqual(self.pos_short.avg_price, 5.99)
        self.assertEqual(self.pos_short.cost, -1797)

        # >>> cover 100 shares at $10, commission $1
        self.pos_short.transact(PositionDirection.BOT, 100, 10, 1)
        self.pos_short.update_market_value(10)
        self.assertEqual(self.pos_short.quantity, -200)
        # 200 shares remaining. unrealized pnl is -200*(10-5.99) = -802
        # negative means loss
        self.assertEqual(self.pos_short.unrealized_pnl, -802)
        # if cover all short position at $10, realized pnl will be
        # 1800 - 3000 - commission = -1204
        self.assertEqual(self.pos_short.realized_pnl, 1800-3000-4)

        # >>> cover 200 shares at $8, commission $1
        # remaining 0 share, short position is closed.
        # realized pnl is 1000+800-1000-1600-commission=1800-2600-5
        # total bought is 300, money amount is 2600
        # total sold is 300, money amount is 1800
        self.pos_short.transact(PositionDirection.BOT, 200, 8, 1)
        self.pos_short.update_market_value(8)

        self.assertEqual(self.pos_short.quantity, 0)
        self.assertEqual(self.pos_short.bought, 300)
        self.assertEqual(self.pos_short.sold, 300)
        self.assertEqual(self.pos_short.total_sold, 1800)
        self.assertEqual(self.pos_short.total_bought, 2600)
        self.assertEqual(self.pos_short.total_commission, 5)
        self.assertEqual(self.pos_short.unrealized_pnl, 0)
        self.assertEqual(self.pos_short.realized_pnl, 1800-2600-5)
        self.assertEqual(self.pos_short.avg_bought_price, 2600/300)


if __name__ == '__main__':
    unittest.main()