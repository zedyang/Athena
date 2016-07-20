import redis
from Athena.utils import filetime_to_dt
from Athena.data_handler.redis_wrapper import RedisWrapper

if __name__ == '__main__':
    r = RedisWrapper(db=1)
    r.flush_db()
    """
    r = redis.Redis()
    sub = r.pubsub()
    sub.subscribe('kl.nanhua.GC1612.clock')
    for message in sub.listen():
        if message['type'] == 'message':
            str_data = message['data'].decode('utf-8')
            list_data = str_data.split('|')
            dict_data = dict(zip(list_data[0::2], list_data[1::2]))
            contract = dict_data['key'].split(':')[0].split('.')[-1]
            print('kl' in dict_data['key'])
            print(dict_data)
    """