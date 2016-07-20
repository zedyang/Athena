import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtGui

from Athena.settings import AthenaConfig
from Athena.db_wrappers.redis_wrapper import RedisWrapper
Kf = AthenaConfig.KLineFields

__author__ = 'zed'


class CandlestickGraphItem(pg.GraphicsObject):
    """
    Kline graphics object.
    """
    def __init__(self, sub_channel, shared_label):
        super(CandlestickGraphItem, self).__init__()

        self.picture = None
        self.ohlc_data = []

        self.sub_channel = sub_channel
        self.__subscribe()

        # shared label
        self.shared_label = shared_label

        # buy/sell signals
        self.added_strategy = False
        self.strategy_channel = None
        self.buy_sell_data = []

    def add_buy_sell_signal(self, sub_channel):
        """

        :param sub_channel:
        :return:
        """
        self.added_strategy = True
        self.strategy_channel = sub_channel

    def __subscribe(self):
        """
        open connection to redis.
        :return:
        """
        self.sub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)

    def set_data(self, ohlc_data):
        """

        :param ohlc_data: list of [end_time, open, high, low, close] arrays.
        :return:
        """
        self.ohlc_data = ohlc_data
        self.drawPicture()

    def drawPicture(self):
        """

        :return:
        """
        # make picture and painter
        self.picture = QtGui.QPicture()
        p = QtGui.QPainter(self.picture)

        half_width = 0.3
        for (open_time, end_time, open_price,
             high_price, low_price, close_price, t) in self.ohlc_data:

            # set pen and brush colors
            if open_price > close_price:
                p.setPen(pg.mkPen('r'))
                p.setBrush(pg.mkBrush('r'))
            else:
                p.setPen(pg.mkPen('g'))
                p.setBrush(pg.mkBrush('g'))

            # draw candlestick bars
            if high_price != low_price:
                p.drawLine(
                    QtCore.QPointF(t, low_price),
                    QtCore.QPointF(t, high_price)
                )
                p.drawRect(QtCore.QRectF(
                    t-half_width, open_price,
                    2*half_width, close_price-open_price
                ))
            else:
                p.setPen(pg.mkPen('w'))
                p.drawLine(
                    QtCore.QPointF(t-half_width, close_price),
                    QtCore.QPointF(t+half_width, close_price)
                )

        # draw orders
        for (update_time, direction, price, t) in self.buy_sell_data:
            p.setPen(pg.mkPen('y'))
            p.drawLine(
                QtCore.QPointF(t-0.4, price),
                QtCore.QPointF(t+0.4, price)
            )
        # connect open/close positions
        for i in range(len(self.buy_sell_data))[1::2]:
            p.setPen(pg.mkPen('y', style=QtCore.Qt.DashLine))
            p.setOpacity(0.6)
            p.drawLine(
                QtCore.QPointF(self.buy_sell_data[i-1][-1],
                               self.buy_sell_data[i-1][-2]),
                QtCore.QPointF(self.buy_sell_data[i][-1],
                               self.buy_sell_data[i][-2])
            )

        p.end()

    def paint(self, p, *args):
        """     """
        if self.ohlc_data:
            p.drawPicture(0, 0, self.picture)

    def boundingRect(self):
        """     """
        if not self.picture:
            return QtCore.QRectF()
        return QtCore.QRectF(self.picture.boundingRect())

    def update(self):
        """
        update plot.
        :return:
        """
        # candles
        keys_to_update_bars = self.sub_wrapper.get_keys(
            '{}:*'.format(self.sub_channel))

        for dict_key in keys_to_update_bars:
            dict_data = self.sub_wrapper.get_dict(dict_key)
            # append row to ohlc data.
            row = [
                dict_data[Kf.open_time], dict_data[Kf.end_time],
                float(dict_data[Kf.open_price]),
                float(dict_data[Kf.high_price]),
                float(dict_data[Kf.low_price]),
                float(dict_data[Kf.close_price]),
                int(dict_data[Kf.count])
            ]
            self.ohlc_data.append(row)

            # delete key
            self.sub_wrapper.connection.delete(dict_key)

        # buy/sell signals
        if self.added_strategy:
            keys_to_update_orders = self.sub_wrapper.get_keys(
                '{}:*'.format(self.strategy_channel))

            for dict_key in keys_to_update_orders:
                dict_data = self.sub_wrapper.get_dict(dict_key)
                # append row
                row = [
                    dict_data['update_time'],
                    dict_data['direction'],
                    float(dict_data['price']),
                    int(dict_data['bar_count'])
                ]
                self.buy_sell_data.append(row)
                # delete key
                self.sub_wrapper.connection.delete(dict_key)

        self.setPos(-len(self.ohlc_data), 0)
        self.drawPicture()

    def update_label_on_mouse_move(self, mouse_point_x):
        """
        called in on_mouse_move method in AthenaMainWindowController.
        :param mouse_point_x:
        :return:
        """
        index = -round(mouse_point_x)
        if 0 < index <= len(self.ohlc_data):
            # find this kline array corresponding to mouse position.
            arr = self.ohlc_data[len(self.ohlc_data)-index]

            self.shared_label.setText(
                "<span style='font-size: 13pt'>KLINE [{time1} -> {time2}]"
                "[{i}]</span> = "
                "<span style='font-size: 13pt' style='color: {color}'>"
                "{o}, {h}, {l}, {c}</span> ".format(
                    time1=arr[0],
                    time2=arr[1],
                    i=arr[-1],
                    color='green' if arr[5] >= arr[2] else 'red',
                    o=arr[2],
                    h=arr[3],
                    l=arr[4],
                    c=arr[5]
                )
            )
