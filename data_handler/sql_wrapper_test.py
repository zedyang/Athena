import unittest
from datetime import datetime

from Athena.data_handler.sql_wrapper import SQLWrapper

if __name__ == '__main__':
    s = SQLWrapper()
    s.make_daily_storage_kl_table('blp_kl')
    s.make_daily_storage_md_table('blp_md')