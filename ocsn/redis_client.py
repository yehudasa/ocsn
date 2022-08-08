import redis
from .ocsn_err import *
from redis.commands.json.path import Path



class RedisClient:
    def __init__(self):
        self.client = redis.Redis(host='localhost', port=6379, db=0)

    def get(self, key):
        result = self.client.json().get(key)

        return result
            
    def put(self, key, data, exclusive = None, only_modify = None):
        self.client.json().set(key, Path.root_path(), data, nx = exclusive, xx = only_modify)

    def remove(self, key):
        self.client.json().delete(key)

    def list(self, prefix = ''):
        cursor = ''
        while cursor != 0:
            cursor, keys = self.client.scan(cursor=cursor, match=prefix + '*')

            for k in keys:
                yield self.get(k)


redis_client = RedisClient()
