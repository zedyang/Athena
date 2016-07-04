import unittest
from datetime import datetime
from decimal import Decimal

from Athena.apis.mssql_api import SQLServerAPI

__author__ = 'zed'


class TestMSSQLAPI(unittest.TestCase):
    """
    tests for SQL Server API
    """
    def setUp(self):
        self.api = SQLServerAPI()
        self.test_rows_1 = [
            (1, 'John Smith', 'John Doe'),
             (2, 'Jane Doe', 'Joe Dog'),
             (3, 'Mike T.', 'Sarah H.')]

        self.test_rows_tick = [
            ('Au(T+D)', '2014-11-10', '08:45:04.0000000',
             '2014-11-10 08:45:04.000', 'QUOTE', 'ASK', Decimal('230.65'),
             3000, Decimal('0.00')),
            ('Au(T+D)', '2014-11-10', '08:45:04.0000000',
             '2014-11-10 08:45:04.000', 'QUOTE', 'BID', Decimal('230.46'),
             2000, Decimal('0.00')),
            ('Au(T+D)', '2014-11-10', '09:00:13.0000000',
             '2014-11-10 09:00:13.000', 'QUOTE', 'ASK', Decimal('230.65'),
             2000, Decimal('0.00')),
            ('Au(T+D)', '2014-11-10', '09:00:13.0000000',
             '2014-11-10 09:00:13.000', 'QUOTE', 'BID', Decimal('230.43'),
             1000, Decimal('0.00')),
            ('Au(T+D)', '2014-11-10', '09:00:13.0000000',
             '2014-11-10 09:00:13.000', 'TRADE', 'NEW', Decimal('230.50'),
             174000, Decimal('0.00'))
        ]

        self.test_rows_bar = [
            ('GC1 Comdty', '2015-08-31', '06:00:00', '2015-08-14 15:00:00.000',
             Decimal('1133.50'), Decimal('1132.70'), Decimal('1132.30'),
             Decimal('1133.20'), 129, Decimal('146125.38')),
            ('GC1 Comdty', '2015-08-31', '06:01:00', '2015-08-14 15:00:00.000',
             Decimal('1133.10'), Decimal('1133.10'), Decimal('1132.90'),
             Decimal('1133.00'), 19, Decimal('21526.20')),
            ('GC1 Comdty', '2015-08-31', '06:02:00', '2015-08-14 15:00:00.000',
             Decimal('1133.20'), Decimal('1133.20'), Decimal('1133.10'),
             Decimal('1133.10'), 10, Decimal('11331.60')),
            ('GC1 Comdty', '2015-08-31', '06:03:00', '2015-08-14 15:00:00.000',
             Decimal('1133.20'), Decimal('1133.10'), Decimal('1133.10'),
             Decimal('1133.20'), 8, Decimal('9065.20')),
            ('GC1 Comdty', '2015-08-31', '06:04:00', '2015-08-14 15:00:00.000',
             Decimal('1133.60'), Decimal('1133.30'), Decimal('1133.30'),
             Decimal('1133.40'), 9, Decimal('10201.00'))
        ]

    def test_queries(self):
        """
        :return:
        """
        # the belows are from the test case 1 in official docs
        cursor = self.api.cursor  # make a shortcut reference

        # create a table.
        cursor.execute("""
        IF OBJECT_ID('Athena_debug_basic', 'U') IS NOT NULL
            DROP TABLE Athena_debug_basic
        CREATE TABLE Athena_debug_basic (
            id INT NOT NULL,
            name VARCHAR(100),
            sales_rep VARCHAR(100),
            PRIMARY KEY(id)
        )""")

        # insert some data into the table
        cursor.executemany(
            "INSERT INTO Athena_debug_basic VALUES (%d, %s, %s)",
            self.test_rows_1)
        # you must call commit() to persist your data
        # if you don't set autocommit to True
        self.api.connection.commit()

        # fetch data from the table
        cursor.execute("SELECT * FROM Athena_debug_basic")
        rows = [row for row in cursor]
        # print(rows)
        self.assertEqual(self.test_rows_1, rows)
        # assert equal for all rows.
        self.api.logout()

    def test_queries_2(self):
        """
        Test sql api upon the market data we already have.
        :return:
        """
        cursor = self.api.cursor
        # select top 5 Au(T+D) tick records.
        cursor.execute("""
        SELECT TOP 5 * FROM tick_history
        WHERE SECURITY='Au(T+D)'
        """)
        rows = [row for row in cursor]
        self.assertEqual(rows, self.test_rows_tick)

    def test_queries_3(self):
        """
        Test sql api upon top 5 GC1 Commodity 1-min bars
        :return:
        """
        cursor = self.api.cursor
        # select top 5 Au(T+D) tick records.
        cursor.execute("""
        SELECT TOP 5 * FROM one_minute_history
        WHERE SECURITY='GC1 Comdty'
        """)
        rows = [row for row in cursor]
        self.assertEqual(rows, self.test_rows_bar)

    def test_fetching_date_section(self):
        """
        Fetch historical data section from SQL server
        with specified time range and instrument list.
        :return:
        """
        instruments_list = ['Au(T+D)', 'au1606']
        begin_time = datetime(2015, 8, 6, 9, 0, 0)  # 2015-08-06 09:00:00
        end_time = datetime(2015, 8, 7, 14, 0, 0)  # 2015-08-07 14:00:00
        data = self.api.select_market_data(instruments_list, begin_time, end_time)

        self.assertEqual(len(data), 24785)
        # check some data rows.
        self.assertEqual(data[0],
                         ('au1606', '2015-08-06', '09:00:00.0000000',
                          '2015-08-06 09:00:00.000', 'QUOTE', 'ASK',
                          Decimal('221.30'), 5, Decimal('0.00')))
        self.assertEqual(data[24784],
                         ('au1606', '2015-08-07', '14:00:00.0000000',
                          '2015-08-07 14:00:00.000', 'QUOTE', 'BID',
                          Decimal('221.85'), 4, Decimal('0.00')))
        self.assertEqual(data[123],
                         ('au1606', '2015-08-06', '09:02:14.0000000',
                          '2015-08-06 09:02:14.000', 'QUOTE', 'ASK',
                          Decimal('221.30'), 5, Decimal('0.00')))
        self.assertEqual(data[9600],
                         ('Au(T+D)', '2015-08-06', '22:28:45.0000000',
                          '2015-08-06 22:28:45.000', 'QUOTE', 'ASK',
                          Decimal('217.90'), 2000, Decimal('0.00')))
        self.assertEqual(data[12951],
                         ('Au(T+D)', '2015-08-07', '00:11:04.0000000',
                          '2015-08-07 00:11:04.000', 'QUOTE', 'BID',
                          Decimal('217.96'), 3000, Decimal('0.00')))

if __name__ == '__main__':
    unittest.main()
