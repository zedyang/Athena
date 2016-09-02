import numpy as np
import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtGui

from Athena.settings import AthenaConfig
from Athena.data_handler.redis_wrapper import RedisWrapper

from Athena.graphics_items.time_axis_item import TimeAxisItem
from Athena.graphics_items.candlestick_item import CandlestickGraphItem
from Athena.graphics_items.market_profile_item import MarketProfileItem
from Athena.graphics_items.curve_item import CurveGraphItem
from Athena.graphics_items.strategy_table_widget import StrategyTableWidget
from Athena.graphics_items.quotes_screen import QuotesTableWidget


__author__ = 'zed'


class AthenaMainWindowController(object):
    """

    """
    def __init__(self, instrument):
        """
        constructor.
        :return:
        """
        self.instrument = instrument
        # flush db first
        self.__flush_db()

        # make qt application, build main window
        self.app = QtGui.QApplication([])
        self.win = QtGui.QWidget()
        self.win.setWindowTitle('Athena_0.0.1')

        self.main_layout = QtGui.QHBoxLayout()
        self.win.setLayout(self.main_layout)

        # arrange left and right layout parts
        self.right_vbox = QtGui.QVBoxLayout()
        self.quote_band = QuotesTableWidget(1, 2)
        self.quote_band.setMaximumWidth(220)

        self.main_layout.addWidget(self.quote_band)
        self.main_layout.addLayout(self.right_vbox)

        # configure the band on the left
        self.quote_band.add_instrument_md(instrument)
        self.quote_band.add_market_profile(
            sub_channel='signal:mp.kl.GC1608.1m'
        )
        self.quote_band.add_pivot((1330.0, 1324.2, 1318.7, 1336.1, 1342.8))

        # configure the box on the right
        # ------------------------------------------------------------------
        # make shared text label bond to mouse movement
        self.candlestick_label = pg.LabelItem(justify='left')
        self.curve_signals_label = pg.LabelItem(justify='left')

        # build plot widget
        self.plot_widget_1 = pg.GraphicsWindow()

        self.plot_widget_1.addItem(self.candlestick_label)
        self.plot_widget_1.nextRow()
        self.plot_widget_1.addItem(self.curve_signals_label)

        self.plot_1_time_axis = TimeAxisItem(orientation='bottom')
        self.plot_1_time_axis.set_measure(measure=60)

        self.plot_1 = self.plot_widget_1.addPlot(
            axisItems={
                'bottom': TimeAxisItem(orientation='bottom')
            },
            row=2,
            col=0
        )

        # make graphics item objects
        self.candlestick = CandlestickGraphItem(
            shared_label=self.candlestick_label,
            sub_channel='plot:kl:{}.1m'.format(instrument)
        )

        self.mkt_profile = MarketProfileItem(
            sub_channel='plot:signal:mp.kl.{}.1m'.format(instrument)
        )

        self.candlestick.add_buy_sell_signal(
            sub_channel='plot:strategy:cta_1'
        )
        # curve item
        self.signal_curves = CurveGraphItem(
            shared_label=self.curve_signals_label,
        )
        self.signal_curves.add_donchian(
            sub_channel='plot:signal:donchian.20.kl.{}.1m'.format(instrument),
            window=20
        )
        self.signal_curves.add_ma(
            sub_channel='plot:signal:ma.kl.{}.1m'.format(instrument),
            window_widths=[36, 48]
        )
        # add items in plot 1
        self.plot_1.addItem(self.candlestick)
        self.plot_1.addItem(self.mkt_profile)
        self.plot_1.addItem(self.signal_curves)

        # plot event bonding and attributes
        self.plot_1_viewbox = self.plot_1.vb
        self.plot_1.showGrid(x=True, y=True)

        # add cross hairs
        self.__add_cross_hair()

        self.plot_1_mouse_proxy = pg.SignalProxy(
            self.plot_1.scene().sigMouseMoved,
            rateLimit=200, slot=self.on_mouse_move
        )

        # tab wrapper
        self.tab = QtGui.QTabWidget()
        self.tab.addTab(self.plot_widget_1, instrument)
        self.tab.addTab(pg.GraphicsWindow(), 'GC1609')
        self.tab.addTab(pg.GraphicsWindow(), 'Au(T+D)')
        self.tab.addTab(pg.GraphicsWindow(), 'Ag(T+D)')
        self.right_vbox.addWidget(self.tab)

        # ------------------------------------------------------------------
        # build strategy table widget
        self.strategy_table = StrategyTableWidget(2,2)
        self.strategy_table.add_strategy('cta_1')
        self.strategy_table.add_strategy('ma_cross')

        self.right_vbox.addWidget(self.strategy_table)
        self.win.show()

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

        # add lines into plots
        self.plot_1.addItem(self.vertical_line_main)
        self.plot_1.addItem(self.horizontal_line)

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
        if self.mkt_profile:
            self.mkt_profile.update()
        if self.signal_curves:
            self.signal_curves.update()
        if self.strategy_table:
            self.strategy_table.update_data()
        if self.quote_band:
            self.quote_band.update_data()

if __name__ == '__main__':
    athena = AthenaMainWindowController('GC1608')
    QtGui.QApplication.instance().exec_()