

# basic system lib
from Athena.data_handler.data_handler import HermesDataHandler


if __name__ == '__main__':
    h = HermesDataHandler()
    h.add_instrument('Au(T+D)', kline_dur_specifiers=('3s', '1m'))
    h.distribute_data()