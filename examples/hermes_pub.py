from Athena.data_handler.data_handler import HermesDataHandler

__author__ = 'zed'


if __name__ == '__main__':
    data_handler = HermesDataHandler()
    data_handler.add_instrument('GC1608')
    data_handler.replay_single_instrument('GC1608')