from pyqtgraph.Qt import QtGui

from Athena.settings import AthenaConfig
from Athena.data_handler.redis_wrapper import RedisWrapper

__author__ = 'zed'


class StrategyTableWidget(QtGui.QTableWidget):
    """

    """
    class ColorPalette(object):
        """

        """
        red = QtGui.QColor(255, 0, 0)
        crimson = QtGui.QColor(220, 20, 60)
        coral = QtGui.QColor(255, 127, 80)
        salmon = QtGui.QColor(250, 128, 114)
        orange = QtGui.QColor(255, 165, 0)
        lime_green = QtGui.QColor(50, 205, 50)
        pale_green = QtGui.QColor(152, 251, 152)
        sea_green = QtGui.QColor(46, 139, 87)
        lime = QtGui.QColor(0, 255, 0)
        gray = QtGui.QColor(128, 128, 128)
        white = QtGui.QColor(255, 255, 255)

        status_mapping = {
            'watch_sell': salmon,
            'open_short': crimson,
            'watch_buy': pale_green,
            'open_long': sea_green,
            'stop_loss': gray,
            'stop_win': gray,
            'watch_cover_long': salmon,
            'cover_long': gray,
            'watch_cover_short': pale_green,
            'cover_short': gray,
            'watch_exit': white
        }

        direction_mapping = {
            'long': sea_green,
            'short': red,
            'None': white
        }

    _header = ['Strategy Name', 'Status', 'Contract',
               'Direction', 'Price', 'Quantity', 'Update Time']

    def __init__(self, *args):
        """

        :param args:
        :param kwargs:
        """
        super(StrategyTableWidget, self).__init__(*args)

        # set header labels
        self.setColumnCount(len(StrategyTableWidget._header))
        self.setHorizontalHeaderLabels(StrategyTableWidget._header)

        # open connection
        self.sub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)

        # containers
        self.sub_channels = dict()
        self.sub_strategies_list = []

        self.table_data = dict()
        self.table_items = dict()
        self.row_mapping = dict()

    def add_strategy(self, strategy_name):
        """

        :param strategy_name:
        :return:
        """
        sub_channel = 'table:strategy:' + strategy_name

        # add sub channels to the list.
        self.sub_channels[strategy_name] = sub_channel
        self.sub_strategies_list.append(strategy_name)

        # add table data place holder, set row counter
        self.table_data[strategy_name] = []
        self.table_items[strategy_name] = []
        i = len(self.sub_strategies_list) - 1
        self.row_mapping[strategy_name] = i

        self.setRowCount(len(self.sub_strategies_list))

        # set new items
        for j in range(len(self._header)):
            new_item = QtGui.QTableWidgetItem('None')
            self.setItem(i, j, new_item)
            self.table_items[strategy_name].append(new_item)

    def __refresh_cells(self):
        """

        :return:
        """
        for k, row in self.table_data.items():
            # set rows
            i = self.row_mapping[k]
            if row:
                for j in range(len(row)):
                    # set text
                    self.table_items[k][j].setText(str(row[j]))

                    # status bg color
                    if j == 1 and (
                        row[j] in
                        StrategyTableWidget.ColorPalette.status_mapping
                    ):
                        self.table_items[k][j].setBackground(
                            StrategyTableWidget.ColorPalette.status_mapping
                            [row[j]]
                        )

                    # set direction text color
                    if j == 3 and (
                        row[j] in
                        StrategyTableWidget.ColorPalette.direction_mapping
                    ):
                        self.table_items[k][j].setTextColor(
                            StrategyTableWidget.ColorPalette.direction_mapping
                            [row[j]]
                        )

    def update_data(self):
        """

        :return:
        """
        for k,v in self.sub_channels.items():

            # keys to update
            keys_to_update = self.sub_wrapper.get_keys('{}:*'.format(v))

            dict_data = dict()

            for dict_key in keys_to_update:
                dict_data = self.sub_wrapper.get_dict(dict_key)
                # delete key
                self.sub_wrapper.connection.delete(dict_key)

            # we only use the last one
            if dict_data:
                row = [
                    dict_data['tag'],        # name
                    dict_data['subtype'],    # status
                    dict_data['contract'],   # contract
                    dict_data['direction'],  # direction
                    dict_data['price'],      # suggested price
                    dict_data['quantity'],
                    dict_data['update_time']
                ]

                self.table_data[k] = row

        self.__refresh_cells()






