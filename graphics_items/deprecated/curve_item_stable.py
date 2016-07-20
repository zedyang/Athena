import pyqtgraph as pg
import numpy as np
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
        self.curve_data = dict()
        self.test_data = []
        self.sub_channel = 'plot:signal:donchian.20.kl.GC1608.1m'
        self.window = 20
        self.__subscribe()

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

        # draw donchian
        for i in range(self.window, len(self.test_data)):
            # set pen and brush colors
            p.setPen(pg.mkPen('g', style=QtCore.Qt.DashLine))
            p.setOpacity(0.6)

            # upper line
            p.drawLine(
                QtCore.QPointF(self.test_data[i-1][-1],
                               self.test_data[i-1][1]),
                QtCore.QPointF(self.test_data[i][-1],
                               self.test_data[i][1])
            )

            # lower line
            p.drawLine(
                QtCore.QPointF(self.test_data[i-1][-1],
                               self.test_data[i-1][3]),
                QtCore.QPointF(self.test_data[i][-1],
                               self.test_data[i][3])
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

        keys_to_update_don = self.sub_wrapper.get_keys(
                '{}:*'.format(self.sub_channel))
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
