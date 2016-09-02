from Athena.data_handler.redis_wrapper import RedisWrapper


if __name__ == '__main__':
    r = RedisWrapper(db=1)
    r.flush_all()
    r.flush_db()
    r.set_dict('GC1612:0000001', {'bid': 100, 'ask': 50})
    r.set_dict('GC1612:0000002', {'bid': 100123, 'ask': 50})
    r.set_dict('GC1612:0000003', {'bid': 10210, 'ask': 50})
    r.set_dict('GC1612:0000005', {'bid': 1030, 'ask': 50})
    r.set_dict('GC1612:0000006', {'bid': 1050, 'ask': 50})

    keys = r.get_keys('GC1612:*')
    print(keys)
    for k in keys:
        print(r.get_dict(k))



