import pandas as pd
import matplotlib.pyplot as plt
plt.style.use('ggplot')

from Athena.data_handler.redis_wrapper import RedisWrapper

if __name__ == '__main__':

    r = RedisWrapper(db=1)
    keys = r.get_keys('portfolio:*')
    log = []
    for k in keys:
        equity = float(r.get_dict(k)['equity'])
        if equity == 0: continue
        log.append(equity)
    plt.plot(log)
    plt.show()

    """
    r = RedisWrapper(db=1)
    keys = r.get_keys('signal:donchian.20.kl.GC1608.1m:*')
    log = []
    log2 = []
    for k in keys:
        equity = float(r.get_dict(k)['up'])
        equity_2 = float(r.get_dict(k)['down'])
        if equity == 0: continue
        log.append(equity)
        log2.append(equity_2)
    plt.plot(log)
    plt.plot(log2)
    plt.show()
    """