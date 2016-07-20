import pyqtgraph as pg
import numpy as np
from pyqtgraph.Qt import QtCore, QtGui

from Athena.settings import AthenaConfig
from Athena.db_wrappers.redis_wrapper import RedisWrapper

__author__ = 'zed'


class CurveGraphItem(pg.GraphicsObject):
    """
    Curve signal graphics object. It is a graphics objects that encapsulates
    multiple curve signals. (like channels, band, and moving averages, etc)

    Initially, the update and drawPicture methods do nothing. There is a
    plot_patterns configuration data structure that specifies the styles and
     patterns to plot each signals (if added to the graph). This configuration
    is a class property of CurveGraphItem. It looks like:

    CurveGraphItem.plot_patterns =
    {
        'ma': {
            'color_short': 'r',
            'color_long': 'g',
            'style': QtCore.Qt.SolidLine,
            'alpha': 0.8
        },
        'donchian': {
            'color_up': 'g',
            'color_down': 'g',
            'style': QtCore.Qt.DashLine,
            'alpha': 0.6
        },
        'signal*': {
            'foo': ...,
             'bar': ...,
             ...
        },
        ...
    }

    constructor should prepare place holders for the signals with pre-compiled
    plotting rules. There should be private variables to contain data,
    subscribe channels and parameters list.

    When signals are added through CurveGraphItem.add_{*}signal() method,
    the corresponding channels in redis are subscribed,
    the plotting flag is set as true and the QTimer update event will update
    the added signals corresponding to plot_option patterns.

    self.curve_data is the internal storage of plotting data
    it is a dict object, something like

    self.curve_data =
    {
        'ma' : [[row1], [row2], [], ...],
        'donchian': [[row1], [row2], [], ...],
        'signal*: [[row1], [row2], [], ...]
    }

    when 'signal*' is included, the drawPicture will iterate through the rows
    in curve_data['signal*'] list, plotting styles and methods are pre-compiled
    according to the characteristics of signal itself.
    """
    plot_patterns = {
        'ma': {
            'color': 'w',
            'colors': ['c', 'm'],
            'color_strings': ['cyan', 'magenta'],
            'style': QtCore.Qt.SolidLine,
            'alpha': 0.5
        },
        'donchian': {
            'color': 'g',
            'style': QtCore.Qt.DashLine,
            'alpha': 0.6
        }
    }

    def __init__(self, shared_label):
        """
        constructor
        :return:
        """
        super(CurveGraphItem, self).__init__()

        self.picture = None
        self.test_data = []
        self.curve_data = dict()

        self.__subscribe()

        # shared label
        self.shared_label = shared_label

        # signals with pre-compiled plotting rules
        # --------------------------------------------------------
        # donchian
        self.added_donchian = False
        self.donchian_sub_channel = None
        self.donchian_window = None
        self.curve_data['donchian'] = []
        # --------------------------------------------------------
        # ma batch
        self.added_ma = False
        self.ma_sub_channel = None
        self.ma_window_widths = None
        self.curve_data['ma'] = []
        # --------------------------------------------------------

    def __subscribe(self):
        """
        open connection to redis.
        :return:
        """
        self.sub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)

    def add_donchian(self, sub_channel, window):
        """
        add donchian channel to the graph.
        :param sub_channel: string
        :param window: integer
        :return:
        """
        self.donchian_sub_channel = sub_channel
        self.donchian_window = window
        self.added_donchian = True

    def add_ma(self, sub_channel, window_widths):
        """
        add moving average curve to the graph.
        :param sub_channel: string
        :param window_widths: list of integers
        :return:
        """
        self.ma_sub_channel = sub_channel
        self.ma_window_widths = window_widths
        self.added_ma = True

    def drawPicture(self):
        """

        :return:
        """
        # make picture and painter
        self.picture = QtGui.QPicture()
        p = QtGui.QPainter(self.picture)

        # --------------------------------------------------------
        # draw donchian
        # set pen and brush colors
        p.setPen(pg.mkPen(
            CurveGraphItem.plot_patterns['donchian']['color'],
            style=CurveGraphItem.plot_patterns['donchian']['style']
        ))
        p.setOpacity(CurveGraphItem.plot_patterns['donchian']['alpha'])
        for i in range(self.donchian_window,
                       len(self.curve_data['donchian'])):
            # upper line
            p.drawLine(
                QtCore.QPointF(self.curve_data['donchian'][i-1][-1],
                               self.curve_data['donchian'][i-1][1]),
                QtCore.QPointF(self.curve_data['donchian'][i][-1],
                               self.curve_data['donchian'][i][1])
            )
            # lower line
            p.drawLine(
                QtCore.QPointF(self.curve_data['donchian'][i-1][-1],
                               self.curve_data['donchian'][i-1][3]),
                QtCore.QPointF(self.curve_data['donchian'][i][-1],
                               self.curve_data['donchian'][i][3])
            )

        # --------------------------------------------------------
        # draw ma
        p.setOpacity(CurveGraphItem.plot_patterns['ma']['alpha'])
        for i in range(max(self.ma_window_widths),
                       len(self.curve_data['ma'])):
            # ma line
            # row = (open_time, ma_1, ma_2, ..., ma_k, count)
            for j in range(len(self.ma_window_widths)):
                p.setPen(pg.mkPen(
                    CurveGraphItem.plot_patterns['ma']['colors'][j],
                    style=CurveGraphItem.plot_patterns['ma']['style']
                ))
                p.drawLine(
                    QtCore.QPointF(self.curve_data['ma'][i-1][-1],
                                   self.curve_data['ma'][i-1][1+j]),
                    QtCore.QPointF(self.curve_data['ma'][i][-1],
                                   self.curve_data['ma'][i][1+j])
                )

        # --------------------------------------------------------
        p.end()

    def paint(self, p, *args):
        """     """
        if self.curve_data['donchian'] or self.curve_data['ma']:
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
        # update donchian channel
        if self.added_donchian:
            keys_to_update_don = self.sub_wrapper.get_keys(
                    '{}:*'.format(self.donchian_sub_channel))
            for dict_key in keys_to_update_don:
                dict_data = self.sub_wrapper.get_dict(dict_key)
                # append row
                row = [
                    dict_data['open_time'],
                    float(dict_data['up']),
                    float(dict_data['middle']),
                    float(dict_data['down']),
                    int(dict_data['count']),
                ]
                self.curve_data['donchian'].append(row)
                # delete key
                self.sub_wrapper.connection.delete(dict_key)
            # relative positioning
            self.setPos(-len(self.curve_data['donchian']), 0)

        # update ma batch
        if self.added_ma:
            keys_to_update_ma = self.sub_wrapper.get_keys(
                '{}:*'.format(self.ma_sub_channel))

            for dict_key in keys_to_update_ma:
                dict_data = self.sub_wrapper.get_dict(dict_key)
                # append row
                row = [dict_data['open_time']]
                for width in self.ma_window_widths:
                    row.append(float(dict_data[str(width)]))
                row.append(int(dict_data['count']))
                self.curve_data['ma'].append(row)
                # delete key
                self.sub_wrapper.connection.delete(dict_key)
            # relative positioning
            self.setPos(-len(self.curve_data['ma']), 0)

        # draw picture
        self.drawPicture()

    def update_label_on_mouse_move(self, mouse_point_x):
        """
        called in on_mouse_move method in AthenaMainWindowController.
        :param event:
        :return:
        """
        index = -round(mouse_point_x)

        # donchian
        donchian_data_length = len(self.curve_data['donchian'])
        if 0 < index <= donchian_data_length:
            # find this signal array corresponding to mouse position.
            arr = self.curve_data['donchian'][donchian_data_length-index]

            string_1 = """
            <span style='font-size: 13pt' style='color: green'>
            DONCHIAN.{window}
            </span> =
            <span style='font-size: 13pt' style='color: green'>
            {up}, {down}
            </span>  |
            """.format(
                window=self.donchian_window,
                up=arr[1],
                down=arr[3]
            )
        else:
            string_1 = ''

        # ma
        ma_data_length = len(self.curve_data['ma'])
        if 0 < index <= ma_data_length:
            # find this signal array corresponding to mouse position.
            arr = self.curve_data['ma'][ma_data_length-index]
            string_2 = ''
            for j in range(len(self.ma_window_widths)):
                string_2 += """
                <span style='font-size: 13pt' style='color: {color}'>
                MA.{window}
                </span> =
                <span style='font-size: 13pt' style='color: {color}'>
                {value}
                </span>  |
                """.format(
                    window=self.ma_window_widths[j],
                    color=CurveGraphItem.plot_patterns['ma']
                    ['color_strings'][j],
                    value=round(arr[1+j], 2)
                )
        else:
            string_2 = ''

        self.shared_label.setText(string_1+string_2)
