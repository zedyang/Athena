import time
from datetime import datetime

import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtGui

from Athena.settings import AthenaConfig
from Athena.db_wrappers.redis_wrapper import RedisWrapper
from Athena.graphics_items.time_axis_item import TimeAxisItem
from Athena.graphics_items.candlestick_item import CandlestickGraphItem
from Athena.graphics_items.curve_item import CurveGraphItem

__author__ = 'zed'


class AthenaMainWindowController(object):
    """

    """
    def __init__(self):
        """
        constructor.
        :return:
        """
        # flush db first
        self.__flush_db()

        # make window
        self.win = pg.GraphicsWindow()
        self.win.setWindowTitle('Athena_0.0.1')
        self.win.ci.layout.setRowMaximumHeight(3, 320)

        # make shared text label
        self.candlestick_label = pg.LabelItem(justify='left')
        self.curve_signals_label = pg.LabelItem(justify='left')

        self.win.addItem(self.candlestick_label)
        self.win.nextRow()
        self.win.addItem(self.curve_signals_label)

        # make plot widgets 1
        self.plot_1_time_axis = TimeAxisItem(orientation='bottom')
        self.plot_1_time_axis.set_measure(measure=60)

        self.plot_1 = self.win.addPlot(
            axisItems={
                'bottom': self.plot_1_time_axis
            },
            row=2,
            col=0
        )

        self.plot_1_viewbox = self.plot_1.vb

        # make graphics item objects
        # candle stick item
        self.candlestick = CandlestickGraphItem(
            shared_label=self.candlestick_label,
            sub_channel='plot:kl:GC1608.1m'
        )
        self.candlestick.add_buy_sell_signal(
            sub_channel='plot:strategy:cta_1'
        )
        # curve item
        self.signal_curves = CurveGraphItem(
            shared_label=self.curve_signals_label,
        )
        self.signal_curves.add_donchian(
            sub_channel='plot:signal:donchian.20.kl.GC1608.1m',
            window=20
        )
        self.signal_curves.add_ma(
            sub_channel='plot:signal:ma.kl.GC1608.1m',
            window_widths=[36, 48]
        )
        # add items in plot 1
        self.plot_1.addItem(self.candlestick)
        self.plot_1.addItem(self.signal_curves)

        # make plot widgets sharing x with 1
        self.plot_1_common_x = self.win.addPlot(
            row=3,
            col=0,
        )
        self.plot_1_common_x_viewbox = self.plot_1_common_x.vb
        self.plot_1_common_x.setXLink(self.plot_1)
        self.plot_1_common_x.setYLink(self.plot_1)

        # set plots' attributes
        self.plot_1.showGrid(x=True, y=True)
        self.plot_1_common_x.showGrid(x=True, y=True)

        # add cross hairs
        self.__add_cross_hair()

        self.plot_1_mouse_proxy = pg.SignalProxy(
            self.plot_1.scene().sigMouseMoved,
            rateLimit=200, slot=self.on_mouse_move
        )

        # link timer to update method
        self.timer = pg.QtCore.QTimer()
        self.timer.timeout.connect(self.refresh_all)
        self.timer.start(100)

    def __flush_db(self):
        """

        :return:
        """
        self.redis_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)
        self.redis_wrapper.flush_db()

    def __add_cross_hair(self):
        """
        add the cross hair item to main window.
        :return:
        """
        # make some infinite line items
        self.vertical_line_main = pg.InfiniteLine(angle=90, movable=False)
        self.horizontal_line = pg.InfiniteLine(angle=0, movable=False)

        # vertical line in plot 2
        self.vertical_line_sub = pg.InfiniteLine(angle=90, movable=False)

        # add lines into plots
        self.plot_1.addItem(self.vertical_line_main)
        self.plot_1.addItem(self.horizontal_line)
        self.plot_1_common_x.addItem(self.vertical_line_sub)

    def on_mouse_move(self, event):
        """
        on mouse move event
        :param event:
        :return:
        """
        position = event[0]
        if self.plot_1.sceneBoundingRect().contains(position):
            # map view to mouse point
            mouse_point = self.plot_1_viewbox.mapSceneToView(position)

            # reset cross hair positions
            mouse_x = mouse_point.x()
            self.vertical_line_main.setPos(mouse_x)
            self.vertical_line_sub.setPos(mouse_x)
            self.horizontal_line.setPos(mouse_point.y())

            self.candlestick.update_label_on_mouse_move(mouse_x)
            self.signal_curves.update_label_on_mouse_move(mouse_x)

    def refresh_all(self):
        """
        refresh all graphics items
        :return:
        """
        if self.candlestick:
            self.candlestick.update()
        if self.signal_curves:
            self.signal_curves.update()


if __name__ == '__main__':
    athena = AthenaMainWindowController()
    QtGui.QApplication.instance().exec_()
