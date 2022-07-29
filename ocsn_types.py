from abc import abstractmethod

from flask import json
from flask.json import JSONEncoder

from .etcd_client import EtcdClient


ec = EtcdClient()



def decode_list(d, T):
    if d is None:
        return None

    l = []
    for item in d:
        l.append(T.decode(item))

    return l


class OCSNEntity(json.JSONEncoder):

    @abstractmethod
    def encode(self):
        raise NotImplementedError()

    @abstractmethod
    def get_key(self):
        raise NotImplementedError()

    @abstractmethod
    def decode_json(self, s):
        if s is None:
            return None
        d = json.loads(s)
        return self.decode(d)

    @abstractmethod
    def encode(self):
        raise NotImplementedError()

    @abstractmethod
    def decode(self, d):
        raise NotImplementedError()

    def encode_json(self):
        return json.dumps(self.encode())

    def load(self):
        k, v = ec.get(self.get_key())
        return self.decode_json(v)

    def store(self):
        k = self.get_key()
        ec.put(k, self.encode_json())



class OCSNEntityJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, OCSNEntity):
            return obj.encode()
        return super(OCSNEntityJSONEncoder, self).default(obj)


class Credentials(OCSNEntity):
    pass


class S3Credentials(Credentials):

    def __init__(self):
        self.id = None
        self.access_key = None
        self.secret = None

    def encode(self):
        return {'id': self.id,
                'access_key': self.access_key,
                'secret': self.secret }
    
    def decode(self, d):
        self.id = d.get('id')
        self.access_key = d.get('access_key')
        self.secret = d.get('secret')
        return self

class OCSNDataProvider(OCSNEntity):

    def __init__(self):
        self.id = None
        self.type = None
        self.creds = {}
        self.status = None

class OCSNDataPolicy(OCSNEntity):

    def __init__(self):
        pass

    def decode(self, d):
        return None

class OCSNUser(OCSNEntity):

    def __init__(self, id = None, creds = None, buckets = None, data_policy = None):
        self.id = id
        self.creds = creds
        self.buckets = buckets
        self.data_policy = data_policy

    def get_key(self):
        return 'u/' + self.id

    def decode(self, d):
        self.id = d.get('id')
        self.buckets = decode_list(d.get('buckets'), OCSNBucket)
        self.data_policy = OCSNDataPolicy().decode(d.get('data_policy'))
        return self

    def encode(self):
        return {'id': self.id,
                'creds': self.creds,
                'buckets': self.buckets,
                'data_policy': self.data_policy,
                }


class OCSNBucketInstanceMapping(OCSNEntity):
    def __init__(self):
        self.data_provider = None
        self.bucket = None
        self.creds_id = None
        self.data_policy = None

    def encode(self):
        return {'id': self.id,
                'bucket': self.bucket,
                'creds_id': self.creds_id,
                'data_policy': self.data_policy}

    def decode(self, d):
        self.id = d.get('id')
        self.bucket = d.get('bucket')
        self.creds_id = d.get('creds_id')
        self.data_policy = OCSNDataPolicy().decode(d.get('data_policy'))
        return self


class OCSNBucket(OCSNEntity):

    def __init__(self, id = None, mappings = None):
        self.id = id
        self.mappings = mappings
    
    def encode(self):
        return {'id': self.id,
                'mappings': self.mappings}

    def decode(self, d):
        self.id = d.get('id')
        self.mappings = OCSNBucketInstanceMapping().decode(d.get('mappings'))
        return self

