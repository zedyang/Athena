import unittest
from datetime import datetime

from Athena.data_handler.sql_wrapper import SQLWrapper


class TestSQLWrapper(unittest.TestCase):
    """

    """
    def test_transportation(self):
        """

        :return:
        """
        instruments_list = ['au1606']
        begin_time = datetime(2016, 6, 20, 9, 0, 0)
        end_time = datetime(2016, 6, 30, 14, 0, 0)

        transporter = SQLWrapper()
        transporter.transport_hist_data(
            instruments_list,
            begin_time,
            end_time
        )

if __name__ == '__main__':
    unittest.main()