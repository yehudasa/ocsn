from abc import abstractmethod

from flask import json
from flask.json import JSONEncoder

from .redis_client import RedisClient


redis_client = RedisClient()



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
        v = redis_client.get(self.get_key())
        return self.decode_json(v)

    def store(self):
        k = self.get_key()
        redis_client.put(k, self.encode_json())



class OCSNEntityJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, OCSNEntity):
            return obj.encode()
        return super(OCSNEntityJSONEncoder, self).default(obj)


class Credentials(OCSNEntity):
    pass


class OCSNS3Credes(Credentials):

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

    def get_key(self):
        return 'creds/s3/' + self.id

class OCSNDataPolicy(OCSNEntity):

    def __init__(self):
        pass

    def decode(self, d):
        return None

class OCSNUser(OCSNEntity):

    def __init__(self, id = None, name = None, creds = None, vbuckets = None, data_policy = None):
        self.id = id
        self.name = name
        self.creds = creds
        self.vbuckets = vbuckets
        self.data_policy = data_policy

    def get_key(self):
        return 'u/' + self.id

    def decode(self, d):
        self.id = d.get('id')
        self.name = d.get('name')
        self.vbuckets = decode_list(d.get('vbuckets'), OCSNVBucket)
        self.data_policy = OCSNDataPolicy().decode(d.get('data_policy'))
        return self

    def encode(self):
        return {'id': self.id,
                'name': self.name,
                'creds': self.creds,
                'vbuckets': self.vbuckets,
                'data_policy': self.data_policy,
                }


class OCSNBucketInstance(OCSNEntity):
    def __init__(self):
        self.id = None
        self.svc_instance = None
        self.bucket = None
        self.obj_prefix = ''
        self.creds_id = None

    def encode(self):
        return {'id': self.id,
                'svc_instance': self.svc_instance,
                'bucket': self.bucket,
                'obj_prefix': self.obj_prefix,
                'creds_id': self.creds_id}

    def decode(self, d):
        self.id = d.get('id')
        self.svc_instance = d.get('svc_instance')
        self.bucket = d.get('bucket')
        self.obj_prefix = d.get('obj_prefix')
        self.creds_id = d.get('creds_id')
        return self


class OCSNBucketInstanceMapping(OCSNEntity):
    def __init__(self):
        self.bis = None # bucket instances

    def encode(self):
        return {'id': self.id,
                'bis': self.bis }

    def decode(self, d):
        self.id = d.get('id')
        self.bis = decode_list(d.get('bis'), OCSNBucketInstance)
        # self.data_policy = OCSNDataPolicy().decode(d.get('data_policy'))
        return self


class OCSNVBucket(OCSNEntity):

    def __init__(self, id = None, mappings = None):
        self.id = id
        self.name = None
        self.mappings = mappings
    
    def get_key(self):
        return 'b/' + self.id

    def encode(self):
        return {'id': self.id,
                'name': self.name,
                'mappings': self.mappings}

    def decode(self, d):
        self.id = d.get('id')
        self.user = d.get('name')
        self.mappings = OCSNBucketInstanceMapping().decode(d.get('mappings'))
        return self

class OCSNTenant(OCSNEntity):

    def __init__(self, id = None, name = None, users = None, buckets = None):
        self.id = id
        self.name = name
        self.creds = creds
        self.users = users
        self.vbuckets = vbuckets

    def get_key(self):
        return 'tenant/' + self.id

    def decode(self, d):
        self.id = d.get('id')
        self.name = d.get('name')
        self.users = decode_list(d.get('users'), OCSNUser)
        self.vbuckets = decode_list(d.get('vbuckets'), OCSNVBucket)
        return self

    def encode(self):
        return {'id': self.id,
                'name': self.name,
                'users': self.users,
                'vbuckets': self.vbuckets,
                }


class OCSNService(OCSNEntity):
    def __init__(self, id = None, name = None, region = None, endpoint = None):
        self.id = id
        self.name = name
        self.region = region
        self.endpoint = endpoint

    def get_key(self):
        return 'svc/' + self.id

    def decode(self, d):
        self.id = d.get('id')
        self.name = d.get('name')
        self.region = d.get('region')
        self.endpoint = d.get('endpoint')
        return self

    def encode(self):
        return {'id': self.id,
                'name': self.name,
                'region': self.region,
                'endpoint': self.endpoint,
                }


class OCSNServiceInstance(OCSNEntity):
    def __init__(self, id = None, name = None, svc_id = None, buckets = None, creds = None):
        self.id = id
        self.name = name
        self.svc_id = svc_id
        self.buckets = buckets
        self.creds = creds

    def get_key(self):
        return 'dg/' + self.id

    def decode(self, d):
        self.id = d.get('id')
        self.name = d.get('name')
        self.svc = d.get('svc')
        self.buckets = d.get('buckets')
        self.creds = d.get('creds')
        return self

    def encode(self):
        return {'id': self.id,
                'name': self.name,
                'svc': self.svc,
                'buckets': self.buckets,
                'creds': self.creds,
                }


class OCSNDataFlowPolicy:
    def __init__(self, id = None):
        # TODO
        pass




