import json

import numpy as np
import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtGui

from Athena.settings import AthenaConfig
from Athena.data_handler.redis_wrapper import RedisWrapper

__author__ = 'zed'


class OrderGraphItem(pg.ScatterPlotItem):
    """
    buy/sell order graphics object
    """
    def __init__(self, *args, **kwargs):
        super(OrderGraphItem, self).__init__(*args, **kwargs)

        self.sub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)

    def add_strategy(self, sub_channel):
        """

        :param sub_channel:
        :return:
        """
        self.sub_channel = sub_channel

    def update(self, *__args):
        keys_to_update = self.sub_wrapper.get_keys(
                '{}:*'.format(self.donchian_sub_channel))
        for dict_key in keys_to_update:
            dict_data = self.sub_wrapper.get_dict(dict_key)
            # append row
            row = [
                float(dict_data['middle']),
                int(dict_data['count']),
            ]
            self.addPoints(x=row[1], y=row[0])
            # delete key
            self.sub_wrapper.connection.delete(dict_key)
        # relative positioning
        self.setPos(-len(self.curve_data['donchian']), 0)


