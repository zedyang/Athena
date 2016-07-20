import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtGui

from Athena.settings import AthenaConfig
from Athena.data_handler.redis_wrapper import RedisWrapper

__author__ = 'zed'


class CurveGraphItem(pg.GraphicsObject):
    """
    curve signal graphics object.
    """
    def __init__(self):
        super(CurveGraphItem, self).__init__()

        self.picture = None
        self.test_data = []
        self.curve_data = dict()
        # curve data is something like
        # {
        #   'ma' : [[row1], [row2], [], ...],
        #   'donchian': [[row1], [row2], [], ...],
        #   ...
        # }

        self.__subscribe()

        # signals that are not added yet
        # ma batch
        self.ma_sub_channel = None
        self.ma_window_widths = None
        self.added_ma = False

        # donchian
        self.donchian_sub_channel = None
        self.donchian_window = None
        self.added_donchian = False

        self.__add_donchian('plot:signal:donchian.20.kl.GC1608.1m',20)

    def add_ma(self, sub_channel, window_widths):
        """
        add moving average curve to the graph
        :param sub_channel: string
        :param window_widths: list of integers
        :return:
        """
        self.ma_sub_channel = sub_channel
        self.ma_window_widths = window_widths
        self.added_ma = True
        self.curve_data['ma'] = []

    def __add_donchian(self, sub_channel, window):
        """
        add moving average curve to the graph
        :param sub_channel: string
        :param window: integer
        :return:
        """
        self.donchian_sub_channel = sub_channel
        self.donchian_window = window
        self.added_donchian = True
        self.curve_data['donchian'] = []

    def __subscribe(self):
        """
        open connection to redis.
        :return:
        """
        self.sub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)

    def drawPicture(self):
        """

        :return:
        """
        # make picture and painter
        self.picture = QtGui.QPicture()
        p = QtGui.QPainter(self.picture)

        """
        if self.added_donchian and self.curve_data['donchian']:
            # row = (open_time, up, middle, down, count)
            temp_data = self.curve_data['donchian']

            # draw donchian
            for i in range(1, len(temp_data)):
                # set pen and brush colors
                p.setPen(pg.mkPen('g'))

                # upper line
                p.drawLine(
                    QtCore.QPointF(temp_data[i-1][-1], temp_data[i-1][1]),
                    QtCore.QPointF(temp_data[i][-1], temp_data[i][1])
                )

                # lower line
                p.drawLine(
                    QtCore.QPointF(temp_data[i-1][-1], temp_data[i-1][3]),
                    QtCore.QPointF(temp_data[i][-1], temp_data[i][3])
                )
        """

        # row = (open_time, up, middle, down, count)
        temp_data = self.test_data

        # draw donchian
        for i in range(1, len(temp_data)):
            # set pen and brush colors
            p.setPen(pg.mkPen('g'))

            # upper line
            p.drawLine(
                QtCore.QPointF(temp_data[i-1][-1], temp_data[i-1][1]),
                QtCore.QPointF(temp_data[i][-1], temp_data[i][1])
            )

            # lower line
            p.drawLine(
                QtCore.QPointF(temp_data[i-1][-1], temp_data[i-1][3]),
                QtCore.QPointF(temp_data[i][-1], temp_data[i][3])
            )

        p.end()

    def paint(self, p, *args):
        """     """
        if self.test_data:
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
        # update ma batch
        """
        if self.added_ma:
            # if ma curves are added to the graphic item
            keys_to_update_ma = self.sub_wrapper.get_keys(
                '{}:*'.format(self.ma_sub_channel))

            for dict_key in keys_to_update_ma:
                dict_data = self.sub_wrapper.get_dict(dict_key)

                row = [dict_data['open_time']]
                for width in self.ma_window_widths:
                    row.append(dict_data[str(width)])
                row.append(dict_data['count'])
                self.curve_data['ma'].append(row)

                # delete key
                self.sub_wrapper.connection.delete(dict_key)

        # update donchian channel
        if self.added_donchian:
            # if donchian channel is added to the graphic item
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
        """
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
            self.test_data.append(row)

            # delete key
            self.sub_wrapper.connection.delete(dict_key)

        self.setPos(-len(self.test_data), 0)
        self.drawPicture()
