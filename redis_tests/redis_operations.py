import redis
import json

if __name__ == '__main__':
    d = {''}
    conn = redis.StrictRedis(host='localhost', port=6379, db='md')

    dict = {'foo':1, 'bar':2}
    conn.hmset('foo1', dict)
    # s = conn.hgetall('foo1')
    # print(s[b'bar'])
