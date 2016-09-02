import json

import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtGui

from Athena.settings import AthenaConfig
from Athena.data_handler.redis_wrapper import RedisWrapper
Kf = AthenaConfig.HermesKLineFields

__author__ = 'zed'


class MarketProfileItem(pg.GraphicsObject):
    """
    Kline graphics object.
    """

    def __init__(self, sub_channel, shared_label=None, step_size=0.1):
        super(MarketProfileItem, self).__init__()

        self.picture = None
        self.step_size = step_size

        # attributes and data
        self.range_data = []
        self.counts_data = []
        self.open_bar_counts = []
        self.va_indices = []
        self.tot_bar_count = 0

        self.sub_channel = sub_channel
        self.__subscribe()

        # shared label
        self.shared_label = shared_label

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

        p.setPen(pg.mkPen('k'))
        p.setOpacity(0.3)

        half_height = self.step_size / 2

        # loop through all periods
        for i in range(len(self.open_bar_counts)):

            # beginning position of this bar.
            t = self.open_bar_counts[i]
            # va: (poc, val, vah)
            va = self.va_indices[i]

            # counter
            j = 0
            for price, count in zip(self.range_data[i], self.counts_data[i]):
                # adjust brush color for poc and vas
                if j == va[0]:
                    p.setBrush(pg.mkBrush('y'))
                elif va[1] <= j <= va[2]:
                    p.setBrush(pg.mkBrush('g'))
                else:
                    p.setBrush(pg.mkBrush('b'))

                # horizontal position
                tt = t

                # draw candlestick bars
                while count > 0:
                    p.drawRect(QtCore.QRectF(
                        tt-0.5, price-half_height,
                        1, 2*half_height
                    ))
                    # increment to horizontal position,
                    # decrement to count
                    tt += 1
                    count -= 1

                j += 1

        p.end()

    def paint(self, p, *args):
        """     """
        if self.counts_data:
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
        # keys
        keys_to_update_bars = self.sub_wrapper.get_keys(
            '{}:*'.format(self.sub_channel))

        for dict_key in keys_to_update_bars:
            dict_data = self.sub_wrapper.get_dict(dict_key)

            # parse market profile lists
            str_data = json.dumps(dict_data)
            str_data = str_data.replace('"[', '[')
            str_data = str_data.replace(']"', ']')
            str_data = str_data.replace("'", '')
            dict_data = json.loads(str_data)

            # this bar count
            count = int(dict_data['this_bar_count'])
            self.tot_bar_count = max(
                self.tot_bar_count, count
            )

            # print(self.counts_data, self.open_bar_counts, count)
            # initialization, or is the beginning of the next period
            if not self.open_bar_counts or (
                count == int(dict_data['open_bar_count'])
            ):

                self.open_bar_counts.append(count)
                self.range_data.append(
                    [float(x) for x in dict_data['range']]
                )
                self.counts_data.append(
                    [int(x) for x in dict_data['counts']]
                )
                self.va_indices.append((
                    int(dict_data['poc_index']),
                    int(dict_data['val_index']),
                    int(dict_data['vah_index'])
                ))


            else:   # otherwise, current period update.

                self.range_data[-1] = \
                    [float(x) for x in dict_data['range']]
                self.counts_data[-1] = \
                    [int(x) for x in dict_data['counts']]
                self.va_indices[-1] = (
                    int(dict_data['poc_index']),
                    int(dict_data['val_index']),
                    int(dict_data['vah_index'])
                )

            # delete key
            self.sub_wrapper.connection.delete(dict_key)

        self.setPos(-self.tot_bar_count-1, 0)
        self.drawPicture()

    def update_label_on_mouse_move(self, mouse_point_x):
        """
        called in on_mouse_move method in AthenaMainWindowController.
        :param mouse_point_x:
        :return:
        """
        pass
