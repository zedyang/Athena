from datetime import datetime
import time

from Athena.data_handler.backtest_driver import BacktestDriver
from Athena.data_handler.sql_wrapper import SQLWrapper
from Athena.data_handler.redis_wrapper import RedisWrapper

__author__ = 'zed'


def transport_history_data():
    """
    Test the naive signal.
    :return:
    """
    redis_api = RedisWrapper(db=0)
    mssql_wrapper = SQLWrapper()

    begin_time = datetime(2016, 7, 13, 0, 0, 0)
    end_time = datetime(2016, 7, 13, 23, 59, 59)
    instruments_list = ['au1706','ag1607','ag1611','Au(T+D)']

    # begin transport
    mssql_wrapper.transport_hist_data(
        instruments_list, begin_time, end_time
    )


def backtest_pub():
    # begin publishing
    print('backtest pub')
    data_handler = BacktestDriver(['au1706','ag1607','ag1611','Au(T+D)'])
    data_handler.distribute_data()

if __name__ == '__main__':
    # this line is responsible for transporting hist data to redis.
    #transport_history_data()

    # followings are to start backtest driver.
    # when transportation is completed,
    # comment out 39-th line
    # de-comment following lines (45 - 48)
    t_start = time.time()
    backtest_pub()
    t_end = time.time()
    print('finished in {} seconds.'.format(t_end-t_start))

