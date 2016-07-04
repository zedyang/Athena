import redis
import time

if __name__ == '__main__':
    r = redis.StrictRedis()
    ps = r.pubsub()
    ps.subscribe('kl.sim.au1606.clock')
    data = []
    while True:
        message = ps.get_message()
        if message:
            print(message)
