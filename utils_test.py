import unittest
from collections import deque

import numpy as np

from Athena.utils import *
from Athena.settings import AthenaConfig

__author__ = 'zed'


class TestFileTimes(unittest.TestCase):
    """

    """
    def test_dt_to_filetime(self):
        """

        :return:
        """
        ft = 131129606622000115
        dt = filetime_to_dt(ft)
        dt_2 = datetime(2016, 7, 14, 17, 4, 22, 200011)
        self.assertEqual(dt, dt_2)

        ft_2 = dt_to_filetime(dt_2)
        self.assertEqual(ft // 10, ft_2 // 10)

        ft = 131129606463120179
        dt = filetime_to_dt(ft)
        dt_2 = datetime(2016, 7, 14, 17, 4, 6, 312017)
        self.assertEqual(dt, dt_2)

        ft_2 = dt_to_filetime(dt_2)
        self.assertEqual(ft // 10, ft_2 // 10)

        dt_3 = datetime.strptime(
            '2016-07-15 15:00:05', AthenaConfig.dt_format)
        ft_3 = dt_to_filetime(dt_3)

        dt_4 = datetime(2016, 7, 14, 17, 4, 6, 312017)
        dt_4_plus_1min = dt_4 + timedelta(minutes=1)
        dt_4_plus_5min = dt_4 + timedelta(seconds=300)
        dt_4_plus_1sec = dt_4 + timedelta(seconds=1)
        ft_4 = dt_to_filetime(dt_4)
        ft_4_plus_1min = dt_to_filetime(dt_4_plus_1min)
        ft_4_plus_5min = dt_to_filetime(dt_4_plus_5min)
        ft_4_plus_1sec = dt_to_filetime(dt_4_plus_1sec)
        print('plus 1 min: ', ft_4, ft_4_plus_1min)
        print(ft_4_plus_1min-ft_4)
        print(ft_4_plus_1sec-ft_4)
        print(ft_4_plus_5min-ft_4)
        self.assertEqual(ft_4_plus_1sec-ft_4, 10000000)
        self.assertEqual(ft_4_plus_1min-ft_4, 600000000)

    def test_deque(self):
        """

        :return:
        """
        q = deque(maxlen=10)
        q.extend([np.nan]*10)
        q.append(0)
        q.append(1)
        q.append(2)
        q.append(3)
        q.append(4)
        print(q)
        print(list(q)[5::])
        print(list(q)[0::])
        print(np.mean(list(q)[0::]))
        q.append(5)
        q.append(6)
        q.append(7)
        q.append(8)
        q.append(9)
        print(list(q))
        print(list(q)[5::])
        print(list(q)[10-2::])
        q.append(10)
        print(list(q))
        print(list(q)[5::])

    def test_dt_strings(self):
        print(type(float('nan')))
        print(type(np.nan))
        print(round(1.4))
        print(round(1.565785685, 2))
        a = [1,2,3,4,5,6]
        print(a[:4])
        d = {
            'f': np.nan,
            'e': 1,
            'd': 2
        }
        print(not np.nan in d.values())
        names = ['d', 'e', 'f']
        (a,b,c) = (d[name] for name in names)
        print(a,b,c)
        for k, v in d.items():
            print(k,v)

        print('%%%')
        channel = ['md:GC:111', 'md:GC:222']
        signal = [c.replace(':', '.') for c in channel]
        print(signal)

        print(datetime.today().date().strftime('%Y%m%d'),']]]]')
if __name__ == '__main__':
    unittest.main()