import numpy as np
from pyqtgraph.Qt import QtGui

from Athena.settings import AthenaConfig
from Athena.data_handler.redis_wrapper import RedisWrapper
Tf = AthenaConfig.HermesTickFields

__author__ = 'zed'


class QuotesTableWidget(QtGui.QTableWidget):
    """

    """

    class ColorPalette(object):
        """

        """
        # reddish
        red = QtGui.QColor(255, 0, 0)
        crimson = QtGui.QColor(220, 20, 60)
        coral = QtGui.QColor(255, 127, 80)
        salmon = QtGui.QColor(250, 128, 114)

        # yellowish
        orange = QtGui.QColor(255, 165, 0)
        gold = QtGui.QColor(255, 215, 0)
        yellow = QtGui.QColor(255, 255, 0)

        # greenish
        lime_green = QtGui.QColor(50, 205, 50)
        pale_green = QtGui.QColor(152, 251, 152)
        sea_green = QtGui.QColor(46, 139, 87)
        lime = QtGui.QColor(0, 255, 0)

        # blueish
        dodger_blue = QtGui.QColor(30, 144, 255)
        blue = QtGui.QColor(0, 0, 255)
        royal_blue = QtGui.QColor(65, 105, 255)

        # purplish
        blue_violet = QtGui.QColor(138, 43, 226)

        # non
        gray = QtGui.QColor(128, 128, 128)
        white = QtGui.QColor(255, 255, 255)

        items_mapping = {
            'Last Price': gray,
            'POC': gold,
            'DPOC': yellow,
            'VAH': blue,
            'VAL': blue,
            'DVAH': dodger_blue,
            'DVAL': dodger_blue,
            'Pivot': orange,
            'R1': lime_green,
            'R2': lime_green,
            'S1': red,
            'S2': red,
        }

    _header = ['Name', 'Value']
    _tick_references = ['Last Price']
    _mp_references = ['POC', 'DPOC', 'VAL', 'VAH', 'DVAL', 'DVAH']
    _pivot_references = ['Pivot', 'S1', 'S2', 'R1', 'R2']

    def __init__(self, *args):
        """

        :param args:
        :param kwargs:
        """
        super(QuotesTableWidget, self).__init__(*args)

        # set header labels
        self.setColumnCount(len(QuotesTableWidget._header))
        self.setHorizontalHeaderLabels(QuotesTableWidget._header)

        # open connection
        self.sub_wrapper = RedisWrapper(db=AthenaConfig.athena_db_index)

        # containers
        self.table_data = dict()
        self.table_items = []
        self.row_mapping = dict()

        # last price
        self.added_tick = False
        self.tick_sub_channel = None

        # mp
        self.added_mp = False
        self.mp_sub_channel = None
        self.mp_period = 0
        self.mp_prd_reset_flag = False

        # pivot
        self.added_pivot = False

    def add_instrument_md(self, instrument):
        """

        :param instrument:
        :return:
        """
        if not self.added_tick:
            added_references = len(QuotesTableWidget._tick_references)
            for k in QuotesTableWidget._tick_references:
                self.table_data[k] = np.nan

            # sub channel of current mp record
            self.tick_sub_channel = 'md:' + instrument + ':0'

            # add new table items
            num_existed_items = len(self.table_items)
            self.setRowCount(num_existed_items + added_references)

            for i in range(num_existed_items,
                           num_existed_items + added_references):
                name_item, value_item = \
                    QtGui.QTableWidgetItem('None'), \
                    QtGui.QTableWidgetItem('None')

                # push item in the table
                self.setItem(i, 0, name_item),
                self.setItem(i, 1, value_item)

                # append item tuple into table_items
                self.table_items.append((name_item, value_item))

            self.added_tick = True

    def add_market_profile(self, sub_channel):
        """

        :param sub_channel:
        :return:
        """
        # POC, DPOC, VAL, VAH, DVAL, DVAH
        if not self.added_mp:
            added_references = len(QuotesTableWidget._mp_references)
            for k in QuotesTableWidget._mp_references:
                self.table_data[k] = -1

            # sub channel of current mp record
            self.mp_sub_channel = sub_channel + ':0'

            # add new table items
            num_existed_items = len(self.table_items)
            self.setRowCount(num_existed_items + added_references)

            for i in range(num_existed_items,
                           num_existed_items + added_references):
                name_item, value_item = \
                    QtGui.QTableWidgetItem('None'), \
                    QtGui.QTableWidgetItem('None')

                # push item in the table
                self.setItem(i, 0, name_item),
                self.setItem(i, 1, value_item)

                # append item tuple into table_items
                self.table_items.append((name_item, value_item))

            self.added_mp = True

    def add_pivot(self, values):
        """

        :param values:
        :return:
        """
        if not self.added_pivot:
            added_references = len(QuotesTableWidget._pivot_references)
            i = 0
            for k in QuotesTableWidget._pivot_references:
                self.table_data[k] = values[i]
                i += 1

            # add new table items
            num_existed_items = len(self.table_items)
            self.setRowCount(num_existed_items + added_references)

            for i in range(num_existed_items,
                           num_existed_items + added_references):
                name_item, value_item = \
                    QtGui.QTableWidgetItem('None'), \
                    QtGui.QTableWidgetItem('None')

                # push item in the table
                self.setItem(i, 0, name_item),
                self.setItem(i, 1, value_item)

                # append item tuple into table_items
                self.table_items.append((name_item, value_item))

            self.added_pivot = True

    def __refresh_cells(self):
        """

        :return:
        """
        # sort table data by reference values
        assoc_list = list(self.table_data.items())
        assoc_list = sorted(
            assoc_list,
            key=lambda x: x[1],
            reverse=True
        )

        # reset table contents
        i = 0
        for k, val in assoc_list:
            # set rows
            self.table_items[i][0].setText(str(k))
            self.table_items[i][1].setText(str(val))

            # reset color
            self.table_items[i][0].setBackground(
                QuotesTableWidget.ColorPalette.items_mapping[k]
            )
            i += 1

    def update_data(self):
        """

        :return:
        """
        # last price
        if self.added_tick:
            # get latest tick
            latest_tick = self.sub_wrapper.get_dict(self.tick_sub_channel)

            if latest_tick:
                self.table_data['Last Price'] = \
                    float(latest_tick[Tf.last_price])

        # market profile
        if self.added_mp:
            # get latest market profile
            latest_mp = self.sub_wrapper.get_dict(self.mp_sub_channel)

            if latest_mp:
                poc, val, vah = \
                    float(latest_mp['poc']), \
                    float(latest_mp['val']), \
                    float(latest_mp['vah'])

                # set last finished market profile
                if latest_mp['open_bar_count'] == latest_mp['this_bar_count']:
                    if not self.mp_prd_reset_flag:
                        self.table_data['POC'], \
                            self.table_data['VAL'], \
                            self.table_data['VAH'] = \
                            self.table_data['DPOC'], \
                            self.table_data['DVAL'], \
                            self.table_data['DVAH']
                    self.mp_prd_reset_flag = True
                else:
                    self.mp_prd_reset_flag = False

                # set developing market profile.
                self.table_data['DPOC'], \
                    self.table_data['DVAL'], \
                    self.table_data['DVAH'] = poc, val, vah

        self.__refresh_cells()
