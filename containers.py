from enum import Enum
from collections import namedtuple

__author__ = 'zed'


class OrderType(object):
    """
    Position direction enumeration, Bought: 1, Sold: 0
    """
    open_long = 'open_long'
    open_short = 'open_short'
    cover_long = 'cover_long'
    cover_short = 'cover_short'
    open = [open_long, open_short]
    cover = [cover_long, cover_short]
    long = [open_long, cover_short]
    short = [open_short, cover_long]


OrderEvent = namedtuple('OrderEvent', [
    'direction',
    'type',
    'subtype',
    'quantity',
    'price',
    'contract',
    'commission',
    'update_time',
    'bar_count'
])
