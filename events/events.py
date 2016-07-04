from Athena.portfolio.portfolio import PositionDirection
__author__ = 'zed'


class Event(object):
    """
    Base class as an interface to other event types.
    """
    pass

# ------------------------------------------------------------------------- #


class BarEvent(Event):
    """
    Represents the event of receiving a market bar.
    Consisting of open-high-low-close-volume-turnover(OHLCVT) data, and
    datetime fields.
    """
    type = 'BAR'    # event type as cls property.

    def __init__(self, ticker, time, open_price, high_price,
                 low_price, close_price, volume, turnover):
        """
        Initialize the bar event.
        :param ticker: Ticker symbol, e.g. 'IF1601'
        :param time: the bar timestamp.
        :param open_price: as it means.
        :param high_price:
        :param low_price:
        :param close_price:
        :param volume: volume of trading.
        :param turnover: total value of the trade
        :return:
        """
        self.ticker = ticker
        self.time = time
        self.open_price = open_price
        self.high_price = high_price
        self.low_price = low_price
        self.close_price = close_price
        self.volume = volume
        self.turnover = turnover

    def __str__(self):
        formatted = "[Bar] Ticker: {}, Open: {}, High: {}, Low: {}, " \
                    "Close: {}, Volume: {}, Turnover: {}, Time: {}".format(
            str(self.ticker), str(self.open_price), str(self.high_price),
            str(self.low_price), str(self.close_price), str(self.volume),
            str(self.turnover), str(self.time)
        )
        return formatted

    def __repr__(self):
        return str(self)

# ------------------------------------------------------------------------- #


class SignalEvent(Event):
    """
    Represents the event that a trading signal is sent by Strategy module.
    The signal is tested against risk metrics given portfolio status
    by the RiskManager module.
    """
    type = "SIGNAL"

    def __init__(self, ticker, direction):
        """
        Initialize the signal event.
        :param ticker: the ticker symbol.
        :param direction: the trading direction. A PositionDirection object.
        :return:
        """
        self.ticker = ticker
        self.direction = direction

    def __str__(self):
        if self.direction == PositionDirection.BOT:
            formatted = "[Signal] Ticker: {}, " \
                        "Direction: BOT".format(self.ticker)
        else:
            formatted = "[Signal] Ticker: {}, " \
                        "Direction: SLD".format(self.ticker)
        return formatted

    def __repr__(self):
        return str(self)

# ------------------------------------------------------------------------- #


class OrderEvent(Event):
    """
    Represents the order made to the broker.
    The OrderEvent is created in TradeHandler when
        - a SignalEvent(from Strategy) is obtained from queue.
        - risk tests against the Portfolio are passed(in RiskManager).
        - signal is sized to appropriate Position quantity.
    """
    type = "ORDER"

    def __init__(self, ticker, direction, quantity):
        """
        Initialize the order event.
        :param ticker: the ticker symbol.
        :param direction: the order direction. A PositionDirection object.
        :param quantity: order quantity.
        :return:
        """
        self.ticker = ticker
        self.direction = direction
        self.quantity = quantity

    def __str__(self):
        if self.direction == PositionDirection.BOT:
            formatted = "[Order] Ticker: {}, " \
                        "Direction: BOT, " \
                        "Quantity: {}".format(self.ticker, self.quantity)
        else:
            formatted = "[Order] Ticker: {}, " \
                        "Direction: SLD, " \
                        "Quantity: {}".format(self.ticker, self.quantity)
        return formatted

    def __repr__(self):
        return str(self)

# ------------------------------------------------------------------------- #


class FillEvent(Event):
    """
    Represents the returned message from the broker when trade is executed.
    The FillEvent is created in ExecutionHandler with (possibly)
        - slippage considered.
        - commission calculated.
        - time lag.
    """
    type = "FILL"

    def __init__(self, ticker, exchange, time,
                 direction, quantity, price, commission):
        """
        Initialize the fill event.
        :param ticker:
        :param exchange: exchange code
        :param time: timestamp, datetime object.
        :param direction: PositionDirection object
        :param quantity: quantity traded,
        :param price: execution price.
        :param commission: commission at the broker.
        :return:
        """
        self.ticker = ticker
        self.exchange = exchange
        self.time = time
        self.direction = direction
        self.quantity = quantity
        self.price = price
        self.commission = commission

    def __str__(self):
        formatted = "[Fill] Ticker: {}, Exchange: {}, Time: {}, " \
                    "Direction: {}, Quantity: {}, Price: {}, " \
                    "Commission: {}".format(
            str(self.ticker), str(self.exchange), str(self.time),
            str(self.direction), str(self.quantity), str(self.price),
            str(self.commission)
        )
        return formatted

    def __repr__(self):
        return str(self)