import json

import numpy as np
import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtGui

from Athena.settings import AthenaConfig
from Athena.data_handler.redis_wrapper import RedisWrapper

__author__ = 'zed'


class CandlestickGraphItem(pg.GraphicsObject):
    """
    Kline graphics object.
    """
    def __init__(self, bar_channel, order_channel):
        super(CandlestickGraphItem, self).__init__()

        self.picture = None
        self.ohlc_data = []
        self.orders = []

        self.bar_channel = bar_channel
        self.order_channel = order_channel
        self.__subscribe()

    def __subscribe(self):
        """
        open connection to redis.
        :return:
        """
        self.sub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)

    def set_data(self, ohlc_data, orders):
        """

        :param ohlc_data: list of [end_time, open, high, low, close] arrays.
        :return:
        """
        self.ohlc_data = ohlc_data
        self.orders = orders
        self.drawPicture()

    def drawPicture(self):
        """

        :return:
        """
        # make picture and painter
        self.picture = QtGui.QPicture()
        p = QtGui.QPainter(self.picture)

        half_width = 0.3
        for (end_time, open_price,
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
        for i in range(len(self.orders))[1::2]:
            p.setPen(pg.mkPen('y'))
            p.drawLine(
                QtCore.QPointF(self.orders[i-1][3], self.orders[i-1][2]),
                QtCore.QPointF(self.orders[i][3], self.orders[i][2])
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
            '{}:*'.format(self.bar_channel))

        for dict_key in keys_to_update_bars:
            dict_data = self.sub_wrapper.get_dict(dict_key)
            # append row to ohlc data.
            row = [
                dict_data['end_time'],
                float(dict_data['open']), float(dict_data['high']),
                float(dict_data['low']), float(dict_data['close']),
                int(dict_data['count'])
            ]
            self.ohlc_data.append(row)

            # delete key
            self.sub_wrapper.connection.delete(dict_key)

        # orders
        keys_to_update_orders = self.sub_wrapper.get_keys(
            '{}:*'.format(self.order_channel))

        for dict_key in keys_to_update_orders:
            dict_data = self.sub_wrapper.get_dict(dict_key)
            # append row to order data
            row = [
                dict_data['update_time'],
                dict_data['direction'],
                float(dict_data['price']),
                int(dict_data['bar_count'])
            ]
            self.orders.append(row)

            # delete key
            self.sub_wrapper.connection.delete(dict_key)

        self.setPos(-len(self.ohlc_data), 0)
        self.drawPicture()

    def mouse_move(self, evt):
        pos = evt[0]  ## using signal proxy turns original arguments into a tuple
        if p1.sceneBoundingRect().contains(pos):
            mousePoint = vb.mapSceneToView(pos)
            index = int(mousePoint.x())
            #if index > 0 and index < len(data1):
            #    label.setText("<span style='font-size: 12pt'>x=%0.1f,   <span style='color: red'>y1=%0.1f</span>,   <span style='color: green'>y2=%0.1f</span>" % (mousePoint.x(), data1[index], data2[index]))
            vline.setPos(mousePoint.x())
            hline.setPos(mousePoint.y())



win = pg.GraphicsWindow()
p1 = win.addPlot()
win.nextRow()
p2 = win.addPlot()
p2.setXLink(p1)
vb = p1.vb
cdstick = CandlestickGraphItem('plot:signal:kl.GC1608.1m', 'plot:strategy:ma_cross')
p1.addItem(cdstick)

vline = pg.InfiniteLine(angle=90, movable=False)
hline = pg.InfiniteLine(angle=0, movable=False)

p1.addItem(vline)
p1.addItem(hline)
p1.showGrid(x=True, y=True)


proxy = pg.SignalProxy(p1.scene().sigMouseMoved, rateLimit=200, slot=cdstick.mouse_move)

timer = pg.QtCore.QTimer()
timer.timeout.connect(cdstick.update)
timer.start(100)


if __name__ == '__main__':
    QtGui.QApplication.instance().exec_()
