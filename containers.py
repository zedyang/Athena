from collections import namedtuple

__author__ = 'zed'


OrderEvent = namedtuple('OrderEvent', [
    'direction',
    'subtype',
    'quantity',
    'price',
    'contract',
    'commission',
    'update_time',
    'bar_count'
])
