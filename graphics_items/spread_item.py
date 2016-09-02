import pyqtgraph as pg
import numpy as np
from pyqtgraph.Qt import QtCore, QtGui

from Athena.settings import AthenaConfig
from Athena.data_handler.redis_wrapper import RedisWrapper
Kf = AthenaConfig.HermesKLineFields
from Athena.containers import OrderType

__author__ = 'zed'


class SpreadGraphItem(pg.GraphicsObject):
    """

    """

    def __init__(self, shared_label):
        """
        constructor
        :return:
        """
        super(SpreadGraphItem, self).__init__()

        self.picture = None
        self.curve_data = []

        # open connection
        self.sub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)
        self.sub_channels = dict()
        self.spot_leg = None
        self.future_leg = None
        self.pair = None
        self.re_estimation_prd = None
        self.band_width = None

        # shared label
        self.shared_label = shared_label

        # buy/sell signals
        self.added_strategy = False
        self.strategy_channel = None
        self.buy_sell_data = []

    def add_pair(self, sub_channel, pair, estimate_cycle, band_width):
        """

        :param sub_channel:
        :param pair:
        :param estimate_cycle:
        :param band_width:
        :return:
        """
        pair_name = '.'.join(pair)
        self.sub_channels[pair_name] = sub_channel
        self.spot_leg, self.future_leg = pair[0], pair[1]
        self.pair = pair_name
        self.re_estimation_prd = estimate_cycle
        self.band_width = band_width

    def add_buy_sell_signal(self, sub_channel):
        """

        :param sub_channel:
        :return:
        """
        self.added_strategy = True
        self.strategy_channel = sub_channel

    def drawPicture(self):
        """

        :return:
        """
        # make picture and painter
        self.picture = QtGui.QPicture()
        p = QtGui.QPainter(self.picture)

        # --------------------------------------------------------
        for i in range(1, len(self.curve_data)):
            # spread line
            p.setPen(pg.mkPen('w'))

            if self.curve_data[i-1][5] == 'nan':
                continue

            p.drawLine(
                QtCore.QPointF(self.curve_data[i-1][-1],
                               self.curve_data[i-1][1]),
                QtCore.QPointF(self.curve_data[i][-1],
                               self.curve_data[i][1])
            )
            p.setPen(pg.mkPen('y'))
            p.drawLine(
                QtCore.QPointF(self.curve_data[i-1][-1],
                               self.curve_data[i-1][2]),
                QtCore.QPointF(self.curve_data[i][-1],
                               self.curve_data[i][2])
            )

            # mean line
            if i >= self.re_estimation_prd + 1:
                p.setPen(pg.mkPen(
                    'r',
                    style=QtCore.Qt.DashLine
                ))
                p.drawLine(
                    QtCore.QPointF(self.curve_data[i-1][-1],
                                   self.curve_data[i-1][5]),
                    QtCore.QPointF(self.curve_data[i][-1],
                                   self.curve_data[i][5])
                )
                p.drawLine(
                    QtCore.QPointF(self.curve_data[i-1][-1],
                                   self.curve_data[i-1][5]+self.band_width),
                    QtCore.QPointF(self.curve_data[i][-1],
                                   self.curve_data[i][5]+self.band_width)
                )
                p.drawLine(
                    QtCore.QPointF(self.curve_data[i-1][-1],
                                   self.curve_data[i-1][5]-self.band_width),
                    QtCore.QPointF(self.curve_data[i][-1],
                                   self.curve_data[i][5]-self.band_width)
                )

        # --------------------------------------------------------
        # draw orders
        vb_range = self.parentWidget().state['targetRange']
        x_range, y_range = vb_range[0][1] - vb_range[0][0], \
                           vb_range[1][1] - vb_range[1][0]
        ratio = y_range / x_range * 1.82

        for i in range(len(self.buy_sell_data))[1::2]:
            spread = abs(
                self.buy_sell_data[i][3] - self.buy_sell_data[i-1][3]
            )
            t = self.buy_sell_data[i][-1]
            order_type = self.buy_sell_data[i][2]
            bar_spread = self.curve_data[t][1]

            p.setPen(pg.mkPen('y'))
            p.drawLine(
                QtCore.QPointF(t-0.4, spread),
                QtCore.QPointF(t+0.4, spread)
            )

            if order_type in OrderType.short:
                p.setPen(pg.mkPen('r'))
                try:
                    p.drawLine(
                        QtCore.QPointF(t-0.3, bar_spread+0.9*ratio),
                        QtCore.QPointF(t+0.3, bar_spread+0.9*ratio)
                    )
                    p.drawLine(
                        QtCore.QPointF(t, bar_spread+0.3*ratio),
                        QtCore.QPointF(t-0.3, bar_spread+0.9*ratio)
                    )
                    p.drawLine(
                        QtCore.QPointF(t, bar_spread+0.3*ratio),
                        QtCore.QPointF(t+0.3, bar_spread+0.9*ratio)
                    )
                except IndexError:
                    pass

            elif order_type in OrderType.long:
                p.setPen(pg.mkPen('g'))
                try:
                    p.drawLine(
                        QtCore.QPointF(t-0.3, bar_spread-0.9*ratio),
                        QtCore.QPointF(t+0.3, bar_spread-0.9*ratio)
                    )
                    p.drawLine(
                        QtCore.QPointF(t, bar_spread-0.3*ratio),
                        QtCore.QPointF(t-0.3, bar_spread-0.9*ratio)
                    )
                    p.drawLine(
                        QtCore.QPointF(t, bar_spread-0.3*ratio),
                        QtCore.QPointF(t+0.3, bar_spread-0.9*ratio)
                    )
                except IndexError:
                    pass

            else: pass
        p.end()

    def paint(self, p, *args):
        """     """
        if self.curve_data:
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
        # update spread
        if self.pair:
            keys_to_update = self.sub_wrapper.get_keys(
                    '{}:*'.format(self.sub_channels[self.pair]))
            for dict_key in keys_to_update:
                dict_data = self.sub_wrapper.get_dict(dict_key)
                # append row
                row = [
                    dict_data[Kf.close_time],
                    round(float(dict_data['spread_buy']),2),
                    round(float(dict_data['spread_sell']), 2),
                    float(dict_data[self.spot_leg]),
                    float(dict_data[self.future_leg]),
                    'nan' if dict_data['band_mean'] == 'nan'
                        else float(dict_data['band_mean']),
                    int(dict_data[Kf.count]),
                ]

                self.curve_data.append(row)
                # delete key
                self.sub_wrapper.connection.delete(dict_key)

        if self.added_strategy:
            keys_to_update_orders = self.sub_wrapper.get_keys(
                '{}:*'.format(self.strategy_channel))

            for dict_key in keys_to_update_orders:
                dict_data = self.sub_wrapper.get_dict(dict_key)

                # append row
                row = [
                    dict_data['update_time'],
                    dict_data['direction'],
                    dict_data['type'],
                    float(dict_data['price']),
                    int(dict_data['bar_count'])
                ]
                if row[1] in ['long', 'short']:
                    self.buy_sell_data.append(row)

                # delete key
                self.sub_wrapper.connection.delete(dict_key)

        # draw picture
        self.setPos(-len(self.curve_data), 0)
        self.drawPicture()

    def update_label_on_mouse_move(self, mouse_point_x):
        """
        called in on_mouse_move method in AthenaMainWindowController.
        :param mouse_point_x:
        :return:
        """
        index = -round(mouse_point_x)
        if 0 < index <= len(self.curve_data):
            # find this kline array corresponding to mouse position.
            arr = self.curve_data[len(self.curve_data)-index]

            self.shared_label.setText(
                "<span style='font-size: 13pt'>SPREAD [{time}]"
                "[{i}] = {buy}/{sell} | SPOT, FUTURE = {s}, {f}</span>".format(
                    time=arr[0],
                    i=arr[-1],
                    buy=arr[1],
                    sell=arr[2],
                    s=arr[3],
                    f=arr[4]
                )
            )

