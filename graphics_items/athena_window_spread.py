import pyqtgraph as pg
from pyqtgraph.Qt import QtGui

from Athena.settings import AthenaConfig
from Athena.data_handler.redis_wrapper import RedisWrapper

from Athena.graphics_items.time_axis_item import TimeAxisItem
from Athena.graphics_items.spread_item import SpreadGraphItem
from Athena.graphics_items.strategy_table_widget import StrategyTableWidget
from Athena.graphics_items.quotes_screen import QuotesTableWidget


__author__ = 'zed'


class AthenaSpreadWindowController(object):
    """

    """
    def __init__(self):
        """

        """
        # open connection and flush db
        self.redis_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)

        # make main skeletons
        self.app = QtGui.QApplication([])
        self.win = QtGui.QWidget()
        self.win.setWindowTitle('Athena [Spread]')

        self.main_layout = QtGui.QHBoxLayout()
        self.win.setLayout(self.main_layout)

        # arrange left and right layout parts
        self.right_vbox = QtGui.QVBoxLayout()
        self.quote_band = QuotesTableWidget(1, 2)
        self.quote_band.setMaximumWidth(220)

        #self.main_layout.addWidget(self.quote_band)
        self.main_layout.addLayout(self.right_vbox)

        # tab wrapper
        self.tab = QtGui.QTabWidget()
        self.right_vbox.addWidget(self.tab)

        # build strategy table widget
        self.strategy_table = StrategyTableWidget(2,2)
        self.right_vbox.addWidget(self.strategy_table)

        # containers
        self.pairs_list = []
        self.plot_widgets = dict()
        self.plot_items = dict()
        self.gui_events = dict()

        self.win.show()

        # link timer to update method
        self.timer = pg.QtCore.QTimer()
        self.timer.timeout.connect(self.refresh_all)
        self.timer.start(500)

    def add_spread1_instance(self, pair, param_list):
        """

        :param pair:
        :param param_list: [band_width, band_mean, stop_win]
        :return:
        """
        pair_name = '.'.join(pair)
        if pair_name in self.pairs_list:
            return

        self.plot_widgets[pair_name] = pg.GraphicsWindow()
        self.plot_items[pair_name] = dict()
        self.gui_events[pair_name] = dict()

        # label items
        self.plot_items[pair_name]['labels'] = dict()
        self.plot_items[pair_name]['labels']['main'] = \
            pg.LabelItem(justify='left')
        self.plot_items[pair_name]['labels']['signal'] = \
            pg.LabelItem(justify='left')

        # add into widget before the plot
        self.plot_widgets[pair_name].addItem(
            self.plot_items[pair_name]['labels']['main'])
        self.plot_widgets[pair_name].nextRow()
        self.plot_widgets[pair_name].addItem(
            self.plot_items[pair_name]['labels']['signal'])

        # axis item
        self.plot_items[pair_name]['axis'] = \
            TimeAxisItem(orientation='bottom')
        self.plot_items[pair_name]['axis'].set_measure(measure=60)

        # plot item
        self.plot_items[pair_name]['plot'] = \
            self.plot_widgets[pair_name].addPlot(
                axisItems={
                    'bottom': self.plot_items[pair_name]['axis']
                },
                row=2,
                col=0
            )

        self.plot_items[pair_name]['items'] = dict()

        # spread
        self.plot_items[pair_name]['items']['spread'] = \
            SpreadGraphItem(
                shared_label=self.plot_items[pair_name]['labels']['main']
            )
        self.plot_items[pair_name]['items']['spread'].add_pair(
            sub_channel='plot:signal:spread.'+pair_name,
            pair=pair,
            estimate_cycle=param_list[0],
            band_width=param_list[2]
        )
        self.plot_items[pair_name]['items']['spread'].add_buy_sell_signal(
            sub_channel='plot:strategy:spread_1.'+pair_name,
        )

        # mouse event
        self.plot_items[pair_name]['hairs'] = (
            pg.InfiniteLine(angle=90, movable=False),
            pg.InfiniteLine(angle=0, movable=False)
        )
        self.plot_items[pair_name]['plot'].addItem(
            self.plot_items[pair_name]['hairs'][0],
        )
        self.plot_items[pair_name]['plot'].addItem(
            self.plot_items[pair_name]['hairs'][1]
        )
        self.gui_events[pair_name]['mouse_move'] = \
            pg.SignalProxy(
                self.plot_items[pair_name]['plot'].scene().sigMouseMoved,
                rateLimit=200, slot=self.on_mouse_move
            )

        # put together into plot widget
        # put plotItems into plot
        self.plot_items[pair_name]['plot'].addItem(
            self.plot_items[pair_name]['items']['spread']
        )

        self.plot_items[pair_name]['vb'] = \
            self.plot_items[pair_name]['plot'].vb

        self.plot_items[pair_name]['plot'].showGrid(x=True, y=True)

        self.tab.addTab(self.plot_widgets[pair_name], pair_name)

        # add to instrument list
        self.pairs_list.append(pair_name)

    def on_mouse_move(self, event):
        """
        on mouse move event
        :param event:
        :return:
        """
        inst = self.pairs_list[self.tab.currentIndex()]
        position = event[0]

        if self.plot_items[inst]['plot'].sceneBoundingRect(
        ).contains(position):
            # map view to mouse point
            mouse_point = \
                self.plot_items[inst]['vb'].mapSceneToView(position)

            # reset cross hair positions
            mouse_x = mouse_point.x()
            self.plot_items[inst]['hairs'][0].setPos(mouse_x)
            self.plot_items[inst]['hairs'][1].setPos(mouse_point.y())

            self.plot_items[inst]['items'][
                'spread'].update_label_on_mouse_move(mouse_x)

    def refresh_all(self):
        """
        refresh all graphics items
        :return:
        """
        for pair in self.pairs_list:
            if self.plot_items[pair]['items']['spread']:
                self.plot_items[pair]['items']['spread'].update()
