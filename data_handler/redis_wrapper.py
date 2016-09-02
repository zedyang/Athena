import redis

from Athena.settings import AthenaConfig

__author__ = 'zed'


class RedisWrapper(object):
    """
    The implementation of Redis api. Each instance will maintain a connection
    to redis server.

    In current design. we expect to open a connection in each independent
    module/instances that fetches data from the queue (which is now
    represented by redis db), and push event in it.

    (* Note that the default limit of number of connection to redis server
    is about 10000
    (*
    """
    __db_type = 'REDIS'

    def __init__(self, db, port=6379, host_name='localhost'):
        """
        Constructor.
        :param db: str, the name of the database (on initialization)
        :param port: int, port. Default=6378
        :param host_name: str, name of the server host. Default: 'localhost'
        :return:
        """
        self.host_name = host_name
        self.port = port
        self.db_name = db

        # db0 protection
        self.hermes_db_protected = True

        # make the connection.
        self.__login(host_name, port, db)

    def __login(self, host_name, port, db):
        """
        open connection.
        :param host_name:
        :param port:
        :param db:
        :return:
        """
        try:
            self.connection = redis.Redis(host=host_name, port=port, db=db)
        except redis.RedisError:
            print('<Error>[Redis]: Could not open connection to Redis.')

    def __shut_down(self):
        """shut down current connection"""
        self.connection.shutdown()

    def assert_db_index(self, db):
        """
        assure the api connect to targeting database. If not, close connection
        and reconnect to the desired db.
        :param db:
        :return:
        """
        try:
            assert self.db_name == db
        except AssertionError:
            self.__shut_down()
            self.__login(self.host_name, self.port, db)

    def flush_all(self):
        """
        clean-up all keys in all databases of current host.
        :return:
        """
        self.connection.flushall()
        print('[Redis]: Cleaned up keys in all dbs.')

    def flush_db(self):
        """
        clean-up all keys in current db.
        :return:
        """
        # if targeting at protected hermes db (db0), does not allow flushing.
        if self.db_name == AthenaConfig.hermes_db_index \
                and self.hermes_db_protected:
            print('[Redis]: The attempt to flush Hermes db is rejected.')
            return

        self.connection.flushdb()
        print('[Redis]: Cleaned up keys in db_{}.'.format(self.db_name))

    def set_dict(self, key, data):
        """
        Set one hash set by key & input python dictionary data.
        :param key: str, value of the key.
        :param data: dict, the data to be set.
        :return:
        """
        self.connection.hmset(key, data)

    def get_dict(self, key):
        """
        Get one hash set from redis and turn into python dict. Get by key.
        Note that dictionary obtained from Redis are unicode strings,
        we should convert it back to Python str.
        :param key: str, value of the key.
        :return:
        """
        d_byte = self.connection.hgetall(key)   # unicode
        # convert back
        d = dict(zip([k.decode('utf8') for k in d_byte.keys()],
                     [v.decode('utf8') for v in d_byte.values()]))
        return d

    def get_keys(self, pattern='*', sort=True):
        """
        make a list of keys in db that matches (regex) pattern.
        :param pattern: string, regex pattern to match the keys.
        :param sort: boolean, whether to sort the returned keys list.
        :return:
        """
        keys = self.connection.keys(pattern=pattern)
        if sort:
            keys.sort()
        return keys

    def migrate_keys(self, keys_list, target_db):
        """
        migrate list of keys to a different db.
        :param keys_list:
        :param target_db:
        :return:
        """
        if target_db == AthenaConfig.hermes_db_index \
                and self.hermes_db_protected:
            print('[Redis]: The attempt to migrate to Hermes db is rejected.')
            return

        N = len(keys_list)
        counter = 0

        for k in keys_list:
            self.connection.move(k, target_db)

            # increment to counter
            counter += 1
            if not counter % 10000:
                print('[Redis]: Migrated {} %.'.format(
                    round(100 * counter / N, 2)), flush=True)

    def reset_key(self, val_1, val_2):
        """
        change the name of key from val_1 to val_2.
        :param val_1: string
        :param val_2: string, new key value
        :return:
        """
        self.connection.rename(val_1, val_2)
