import pyqtgraph as pg
from PyQt4.QtCore import QTime

__author__ = 'zed'


class TimeAxisItem(pg.AxisItem):
    """
    Axis item with time tick strings.
    """
    def __init__(self, *args, **kwargs):
        """
        constructor
        :param args:
        :param kwargs:
        :return:
        """
        super(TimeAxisItem, self).__init__(*args, **kwargs)

        # set measure
        self.measure = None
        self.set_measure()

    def set_measure(self, measure=60):
        """
        set unit '1' on x axis equal to measure * seconds
        :param measure: integer
        :return:
        """
        self.measure = measure

    def tickStrings(self, values, scale, spacing):
        """
        overwrite tickStrings method.
        :param values:
        :param scale:
        :param spacing:
        :return:
        """
        return [
            QTime().addSecs(
                -value * self.measure).toString('hh:mm:ss')
            for value in values
        ]