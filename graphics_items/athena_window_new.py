import pyqtgraph as pg
from pyqtgraph.Qt import QtGui

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
    loaded_strategies = ['cta1', 'spread1', 'cta2', 'signalmarketvol']

    def __init__(self):
        """

        """
        # open connection and flush db
        self.redis_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)
        self.redis_wrapper.flush_db()

        # make main skeletons
        self.app = QtGui.QApplication([])
        self.win = QtGui.QWidget()
        self.win.setWindowTitle('Athena [Single Instrument]')

        self.main_layout = QtGui.QHBoxLayout()
        self.win.setLayout(self.main_layout)

        # arrange left and right layout parts
        self.right_vbox = QtGui.QVBoxLayout()
        self.tab = QtGui.QTabWidget()
        self.quote_band = QuotesTableWidget(1, 2)
        self.quote_band.setMaximumWidth(220)

        self.main_layout.addWidget(self.quote_band)
        self.main_layout.addLayout(self.right_vbox)

        # tab wrapper
        self.right_vbox.addWidget(self.tab)

        # build strategy table widget
        self.strategy_table = StrategyTableWidget(2, 2)
        self.right_vbox.addWidget(self.strategy_table)

        # containers
        self.batches_list = []
        self.plot_widgets = dict()
        self.plot_items = dict()
        self.gui_events = dict()
        for s in self.loaded_strategies:
            self.plot_widgets[s] = dict()
            self.plot_items[s] = dict()
            self.gui_events[s] = dict()

        self.win.show()

        # link timer to update method
        self.timer = pg.QtCore.QTimer()
        self.timer.timeout.connect(self.refresh_all)
        self.timer.start(500)

    def configure_quote_band(self, instrument, mp_channel):
        self.quote_band.add_instrument_md(instrument)
        self.quote_band.add_market_profile(mp_channel)

    def add_cta1_instance(self,
                          instrument,
                          params_list,
                          plot_channel_index=-1,
                          duration_specifier='1m'):
        """

        :param instrument:
        :param params_list:
        :param plot_channel_index:
        ['ma_short', 'ma_long', 'don', 'stop_win', 'break_control']
        :param duration_specifier:
        :return:
        """
        if plot_channel_index >= 0:
            plot_channel_prefix = 'plot_' + str(plot_channel_index)
        else:
            plot_channel_prefix = 'plot'

        batch_name = 'cta1:' + instrument
        if batch_name in self.batches_list:
            return

        self.plot_widgets['cta1'][instrument] = pg.GraphicsWindow()
        self.plot_items['cta1'][instrument] = dict()
        self.gui_events['cta1'][instrument] = dict()

        widget_node, item_node, event_node = \
            self.plot_widgets['cta1'][instrument], \
            self.plot_items['cta1'][instrument], \
            self.gui_events['cta1'][instrument]

        # label items
        item_node['labels'] = dict()
        item_node['labels']['main'] = pg.LabelItem(justify='left')
        item_node['labels']['signal'] = pg.LabelItem(justify='left')

        # add into widget before the plot
        widget_node.addItem(item_node['labels']['main'])
        widget_node.nextRow()
        widget_node.addItem(item_node['labels']['signal'])

        # axis item
        item_node['axis'] = TimeAxisItem(orientation='bottom')
        item_node['axis'].set_measure(measure=60)

        # plot item
        item_node['plot'] = widget_node.addPlot(
            axisItems={'bottom': item_node['axis']},
            row=2,
            col=0
        )

        item_node['items'] = dict()

        # candle stick
        item_node['items']['candle'] = \
            CandlestickGraphItem(
                shared_label=item_node['labels']['main'],
                sub_channel='{}:kl:{}.{}'.format(
                    plot_channel_prefix,
                    instrument,
                    duration_specifier
                )
            )
        item_node['items']['candle'].add_buy_sell_signal(
            sub_channel='plot:strategy:cta_1.{}'.format(
                instrument
            )
        )

        # mkt profile
        item_node['items']['mp'] = \
            MarketProfileItem(
                sub_channel='{}:signal:mp.{}.{}'.format(
                    plot_channel_prefix,
                    instrument,
                    duration_specifier
                ),
                step_size=AthenaConfig.hermes_tick_size_mapping[instrument]
            )

        # curves
        item_node['items']['curves'] = \
            CurveGraphItem(
                shared_label=item_node['labels']['signal'],
            )
        item_node['items']['curves'].add_donchian(
            sub_channel='{}:signal:donchian.{}.{}'.format(
                plot_channel_prefix,
                instrument,
                duration_specifier
            ),
            window_widths=[params_list[2], params_list[2]]
        )
        item_node['items']['curves'].add_ma(
            sub_channel='{}:signal:ma.{}.{}'.format(
                plot_channel_prefix,
                instrument,
                duration_specifier
            ),
            window_widths=[params_list[0], params_list[1]]
        )

        # mouse event
        item_node['hairs'] = (
            pg.InfiniteLine(angle=90, movable=False),
            pg.InfiniteLine(angle=0, movable=False)
        )
        item_node['plot'].addItem(item_node['hairs'][0])
        item_node['plot'].addItem(item_node['hairs'][1])

        event_node['mouse_move'] = \
            pg.SignalProxy(
                item_node['plot'].scene().sigMouseMoved,
                rateLimit=200, slot=self.on_mouse_move
            )

        # put together into plot widget
        # put plotItems into plot
        item_node['plot'].addItem(item_node['items']['candle'])
        item_node['plot'].addItem(item_node['items']['mp'])
        item_node['plot'].addItem(item_node['items']['curves'])

        item_node['vb'] = item_node['plot'].vb

        item_node['plot'].showGrid(x=True, y=True)

        self.tab.addTab(widget_node, batch_name)
        self.strategy_table.add_strategy('cta_1.' + instrument)

        # add to instrument list
        self.batches_list.append(batch_name)

    def add_cta2_instance(self,
                          instrument,
                          params_list,
                          plot_channel_index=-1,
                          duration_specifier='1m'):
        """

        :param instrument:
        :param params_list:
        ['ma', 'don_up', 'don_down', 'stop_win', 'trailing']
        :param plot_channel_index:
        :param duration_specifier:
        :return:
        """
        if plot_channel_index >= 0:
            plot_channel_prefix = 'plot_' + str(plot_channel_index)
        else:
            plot_channel_prefix = 'plot'

        batch_name = 'cta2:' + instrument
        if batch_name in self.batches_list:
            return

        self.plot_widgets['cta2'][instrument] = pg.GraphicsWindow()
        self.plot_items['cta2'][instrument] = dict()
        self.gui_events['cta2'][instrument] = dict()

        widget_node, item_node, event_node = \
            self.plot_widgets['cta2'][instrument], \
            self.plot_items['cta2'][instrument], \
            self.gui_events['cta2'][instrument]

        # label items
        item_node['labels'] = dict()
        item_node['labels']['main'] = pg.LabelItem(justify='left')
        item_node['labels']['signal'] = pg.LabelItem(justify='left')

        # add into widget before the plot
        widget_node.addItem(item_node['labels']['main'])
        widget_node.nextRow()
        widget_node.addItem(item_node['labels']['signal'])

        # axis item
        item_node['axis'] = TimeAxisItem(orientation='bottom')
        item_node['axis'].set_measure(measure=60)

        # plot item
        item_node['plot'] = widget_node.addPlot(
            axisItems={'bottom': item_node['axis']},
            row=2,
            col=0
        )

        item_node['items'] = dict()

        # candle stick
        item_node['items']['candle'] = \
            CandlestickGraphItem(
                shared_label=item_node['labels']['main'],
                sub_channel='{}:kl:{}.{}'.format(
                    plot_channel_prefix,
                    instrument,
                    duration_specifier
                )
            )
        item_node['items']['candle'].add_buy_sell_signal(
            sub_channel='plot:strategy:cta_2.{}'.format(
                instrument
            )
        )

        # mkt profile
        item_node['items']['mp'] = \
            MarketProfileItem(
                sub_channel='{}:signal:mp.{}.{}'.format(
                    plot_channel_prefix,
                    instrument,
                    duration_specifier
                ),
                step_size=AthenaConfig.hermes_tick_size_mapping[instrument]
            )

        # curves
        item_node['items']['curves'] = \
            CurveGraphItem(
                shared_label=item_node['labels']['signal'],
            )
        item_node['items']['curves'].add_donchian(
            sub_channel='{}:signal:donchian.{}.{}'.format(
                plot_channel_prefix,
                instrument,
                duration_specifier
            ),
            window_widths=[params_list[1], params_list[2]]
        )
        item_node['items']['curves'].add_ma(
            sub_channel='{}:signal:ma.{}.{}'.format(
                plot_channel_prefix,
                instrument,
                duration_specifier
            ),
            window_widths=[params_list[0]]
        )

        # mouse event
        item_node['hairs'] = (
            pg.InfiniteLine(angle=90, movable=False),
            pg.InfiniteLine(angle=0, movable=False)
        )
        item_node['plot'].addItem(item_node['hairs'][0])
        item_node['plot'].addItem(item_node['hairs'][1])

        event_node['mouse_move'] = \
            pg.SignalProxy(
                item_node['plot'].scene().sigMouseMoved,
                rateLimit=200, slot=self.on_mouse_move
            )

        # put together into plot widget
        # put plotItems into plot
        item_node['plot'].addItem(item_node['items']['candle'])
        item_node['plot'].addItem(item_node['items']['mp'])
        item_node['plot'].addItem(item_node['items']['curves'])

        item_node['vb'] = item_node['plot'].vb

        item_node['plot'].showGrid(x=True, y=True)

        self.tab.addTab(widget_node, batch_name)
        self.strategy_table.add_strategy('cta_2.' + instrument)

        # add to instrument list
        self.batches_list.append(batch_name)

    def add_signal_marketvol(self,
                             instrument,
                             params_list,
                             plot_channel_index=-1,
                             duration_specifier='1m'):
        """

        :param instrument:
        :param params_list:
        ['ma', 'don_up', 'don_down', 'stop_win', 'trailing']
        :param plot_channel_index:
        :param duration_specifier:
        :return:
        """
        if plot_channel_index >= 0:
            plot_channel_prefix = 'plot_' + str(plot_channel_index)
        else:
            plot_channel_prefix = 'plot'

        batch_name = 'signalmarketvol:' + instrument
        if batch_name in self.batches_list:
            return

        self.plot_widgets['signalmarketvol'][instrument] = pg.GraphicsWindow()
        self.plot_items['signalmarketvol'][instrument] = dict()
        self.gui_events['signalmarketvol'][instrument] = dict()

        widget_node, item_node, event_node = \
            self.plot_widgets['signalmarketvol'][instrument], \
            self.plot_items['signalmarketvol'][instrument], \
            self.gui_events['signalmarketvol'][instrument]

        # label items
        item_node['labels'] = dict()
        item_node['labels']['main'] = pg.LabelItem(justify='left')
        item_node['labels']['signal'] = pg.LabelItem(justify='left')

        # add into widget before the plot
        widget_node.addItem(item_node['labels']['main'])
        widget_node.nextRow()
        widget_node.addItem(item_node['labels']['signal'])

        # axis item
        item_node['axis'] = TimeAxisItem(orientation='bottom')
        item_node['axis'].set_measure(measure=60)

        # plot item
        item_node['plot'] = widget_node.addPlot(
            axisItems={'bottom': item_node['axis']},
            row=2,
            col=0
        )

        item_node['items'] = dict()

        # candle stick
        item_node['items']['candle'] = \
            CandlestickGraphItem(
                shared_label=item_node['labels']['main'],
                sub_channel='{}:kl:{}.{}'.format(
                    plot_channel_prefix,
                    instrument,
                    duration_specifier
                )
            )
        item_node['items']['candle'].add_buy_sell_signal(
            sub_channel='plot:strategy:signal_marketvol.{}'.format(
                instrument
            )
        )

        # mkt profile
        item_node['items']['mp'] = \
            MarketProfileItem(
                sub_channel='{}:signal:mp.{}.{}'.format(
                    plot_channel_prefix,
                    instrument,
                    duration_specifier
                ),
                step_size=AthenaConfig.hermes_tick_size_mapping[instrument]
            )

        # curves
        item_node['items']['curves'] = \
            CurveGraphItem(
                shared_label=item_node['labels']['signal'],
            )
        item_node['items']['curves'].add_donchian(
            sub_channel='{}:signal:donchian.{}.{}'.format(
                plot_channel_prefix,
                instrument,
                duration_specifier
            ),
            window_widths=[params_list[1], params_list[2]]
        )
        item_node['items']['curves'].add_ma(
            sub_channel='{}:signal:ma.{}.{}'.format(
                plot_channel_prefix,
                instrument,
                duration_specifier
            ),
            window_widths=[params_list[0]]
        )

        # mouse event
        item_node['hairs'] = (
            pg.InfiniteLine(angle=90, movable=False),
            pg.InfiniteLine(angle=0, movable=False)
        )
        item_node['plot'].addItem(item_node['hairs'][0])
        item_node['plot'].addItem(item_node['hairs'][1])

        event_node['mouse_move'] = \
            pg.SignalProxy(
                item_node['plot'].scene().sigMouseMoved,
                rateLimit=200, slot=self.on_mouse_move
            )

        # put together into plot widget
        # put plotItems into plot
        item_node['plot'].addItem(item_node['items']['candle'])
        item_node['plot'].addItem(item_node['items']['mp'])
        item_node['plot'].addItem(item_node['items']['curves'])

        item_node['vb'] = item_node['plot'].vb

        item_node['plot'].showGrid(x=True, y=True)

        self.tab.addTab(widget_node, batch_name)
        self.strategy_table.add_strategy('signal_marketvol.' + instrument)

        # add to instrument list
        self.batches_list.append(batch_name)

    def on_mouse_move(self, event):
        """
        on mouse move event
        :param event:
        :return:
        """
        tab = self.batches_list[self.tab.currentIndex()]
        s_type, symbol = tab.split(':')
        position = event[0]
        item_node = self.plot_items[s_type][symbol]

        if item_node['plot'].sceneBoundingRect(
        ).contains(position):
            # map view to mouse point
            mouse_point = item_node['vb'].mapSceneToView(position)

            # reset cross hair positions
            mouse_x = mouse_point.x()
            item_node['hairs'][0].setPos(mouse_x)
            item_node['hairs'][1].setPos(mouse_point.y())

            item_node['items'][
                'candle'].update_label_on_mouse_move(mouse_x)
            item_node['items'][
                'curves'].update_label_on_mouse_move(mouse_x)

    def refresh_all(self):
        """
        refresh all graphics items
        :return:
        """
        for tab in self.batches_list:
            s_type, symbol = tab.split(':')

            if s_type in ['cta1', 'cta2', 'signalmarketvol']:
                node = self.plot_items[s_type][symbol]
                if node['items']['candle']:
                    node['items']['candle'].update()
                if node['items']['mp']:
                    node['items']['mp'].update()
                # if node['items']['curves']:
                #     node['items']['curves'].update()
                if self.strategy_table:
                    self.strategy_table.update_data()
                if self.quote_band:
                    self.quote_band.update_data()

            elif s_type in ['spread1']:
                pass
