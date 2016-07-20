from enum import Enum

__author__ = 'zed'


class PositionDirection(Enum):
    """
    Position direction enumeration, Bought: 1, Sold: 0
    """
    BOT = '1'   # (we) buy, mkt maker sell (ask)
    SLD = '0'   # (we) sell, mkt maker buy (bid)


class Position(object):
    """
    Position class. This class encapsulates the data related to one specific
    position (one instrument). It records all BOT\SLD actions and associated prices
    and tracks the PnL on this instrument.
    """
    def __init__(self, instrument, direction,
                 init_quantity, init_price, init_commission):
        """
        Create the position object.
        :param instrument: The name of instrument
        :param direction: The direction of initial trade,
            a PositionDirection object.
        :param init_quantity: The quantity associated with initial position.
        :param init_price: The initial price.
        :param init_commission: commission.
        :return:
        relationships between variables
        -------------------------------
        * bought: the amount bought. Regard both Open long and cover short
            as bought.
        * avg_bought_price:
        * total_bought: bought * avg_bought_price
        * avg_price: the price associated with the direction that the Position
            is opened with. Considering the commission.
        * quantity: position (net) quantity; associated with the direction
            that the Position is opened with.
        * cost: avg_price * quantity. (!including commission)
        * net = bought - sold, long position is positive.
        * net_total = total_sold - total_bought, cash inflow is positive.
        """
        self.instrument = instrument
        self.direction = direction
        self.quantity = init_quantity # make positive temporarily,
        # in the end it will have correct sign.
        self.init_price = init_price
        self.init_commission = init_commission

        # track down following things
        # ---------------------------
        # total quantity that has been bought/sold in the lifespan of position.
        self.bought = 0
        self.sold = 0

        # price averaging among all bought/sold transaction.
        self.avg_bought_price = 0
        self.avg_sold_price = 0
        self.avg_price = 0

        # total money amount of bought/sold,
        # = self.bought * self.avg_bought_price
        self.total_bought = 0
        self.total_sold = 0

        # total commission incurred in the lifespan of position.
        self.total_commission = init_commission

        # realized/unrealized PnL
        self.market_value = 0
        self.realized_pnl = 0
        self.unrealized_pnl = 0

        # net variables
        self.net = 0
        self.net_total = 0
        self.net_total_deducing_commission = 0
        self.cost = 0

        # initialize bought/sold, bought/sold_price and total_bought/sold,
        # net quantity, net total money amount, net amount deducing commission
        self.__calculate_init_value()

    def __calculate_init_value(self):
        """
        Initialize some variables when the initial transaction happens.
        Only invoked on construction.
        :return:
        """
        if self.direction == PositionDirection.BOT:  # initialized with bought
            self.bought = self.quantity  # set total bought quantity
            self.avg_bought_price = self.init_price  # set average bought price
            self.total_bought = self.bought * self.avg_bought_price

            # price including commission
            self.avg_price = (self.init_price * self.quantity +
                              self.init_commission) / self.quantity
            # cost basis
            self.cost = self.quantity * self.avg_price
        else:  # initialize with sold (self.direction == PositionDirection.SLD)
            self.sold = self.quantity
            self.avg_sold_price = self.init_price
            self.total_sold = self.sold * self.avg_sold_price
            self.avg_price = (self.init_price * self.quantity -
                              self.init_commission) / self.quantity
            self.cost = -self.quantity * self.avg_price

        # net amount of shares, positive if long
        self.net = self.bought - self.sold
        self.quantity = self.net
        # net monetary amount, positive if cash inflow
        self.net_total = self.total_sold - self.total_bought
        self.net_total_deducing_commission = self.net_total - \
                                             self.init_commission

    def update_market_value(self, market_price):
        """
        update market value by current market price.
         - Since this project is mainly designed for bar data,
           I didn't consider bid/ask price here, which is also easy..
        :param self:
        :param market_price: the current market price
        :return:
        """
        self.market_value = self.quantity * market_price
        self.unrealized_pnl = self.market_value - self.cost
        self.realized_pnl = (
            self.market_value + self.net_total_deducing_commission
        )

    def transact(self, direction, quantity, price, commission):
        """
        update position when new transaction is made.
        :param direction: direction of new transaction.
        :param quantity: as is suggested..
        :param price: ..
        :param commission: ..
        :return:
        """
        self.total_commission += commission

        if direction == PositionDirection.BOT:  # new buy transaction
            self.avg_bought_price = (
                1.0 * (self.avg_bought_price * self.bought +
                       price * quantity) / (self.bought + quantity)
            )
            if self.direction == PositionDirection.BOT:
                # self.direction is that of initial position.
                # If it is the same as direction of new transaction,
                # then the position is added, otherwise it's covered
                self.avg_price = (
                    (self.avg_price*self.bought + price*quantity +
                     commission) / (quantity + self.bought)
                )

            self.bought += quantity
            self.total_bought = self.bought * self.avg_bought_price
        # ---------------------------------------------------------------- #
        else:  # (direction == PositionDirection.SLD), new sell transaction
            self.avg_sold_price = (
                (self.avg_sold_price * self.sold + price * quantity) / (
                    self.sold + quantity)
            )
            if self.direction == PositionDirection.SLD:
                # Mirrors the procedures above.
                self.avg_price = (
                    (self.avg_price*self.sold + price*quantity -
                     commission) / (self.sold + quantity)
                )
            self.sold += quantity
            self.total_sold = self.sold * self.avg_sold_price

        # --------------- #
        # Adjust net variables.
        self.net = self.bought - self.sold
        self.quantity = self.net
        self.net_total = self.total_sold - self.total_bought
        self.net_total_deducing_commission = \
            self.net_total - self.total_commission

        # Adjust cost associated with initial direction
        self.cost = self.quantity * self.avg_price
