import redis
from redis.commands.json.path import Path
from .ocsn_err import OCSNException, OCSNError
 


class RedisClient:
    def __init__(self):
        self.client = redis.Redis(host='localhost', port=6379, db=0)

    def get(self, key):
        result = self.client.json().get(key)

        return result
            
    def put(self, key, data):
        print('redis-json put: key={k} data={d}'.format(k=key, d=data))
        self.client.json().set(key, Path.root_path(), data)

