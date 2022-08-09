from abc import abstractmethod

from flask import json
from flask.json import JSONEncoder

from .redis_client import RedisClient, redis_client





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

    def store(self, exclusive = None, only_modify = None):
        k = self.get_key()
        redis_client.put(k, self.encode_json(), exclusive = exclusive, only_modify = only_modify)

    def remove(self):
        k = self.get_key()
        redis_client.remove(k)



class OCSNEntityJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, OCSNEntity):
            return obj.encode()
        return super(OCSNEntityJSONEncoder, self).default(obj)


class Credentials(OCSNEntity):
    pass


class OCSNS3Creds(Credentials):

    def __init__(self, svci, id = None, access_key = None, secret = None):
        self.svci = svci
        self.id = id
        self.access_key = access_key
        self.secret = secret

    def encode(self):
        return {'svci': self.svci,
                'id': self.id,
                'access_key': self.access_key,
                'secret': self.secret }
    
    def decode(self, d):
        self.svci = d.get('svci')
        self.id = d.get('id')
        self.access_key = d.get('access_key')
        self.secret = d.get('secret')
        return self

    def get_prefix(self):
        return 'creds/' + self.svci + '/s3/'

    def get_key(self):
        return self.get_prefix() + self.id


class OCSNDataPolicy(OCSNEntity):

    def __init__(self):
        pass

    def decode(self, d):
        return None

class OCSNUser(OCSNEntity):

    def __init__(self, tenant_id, id = None, name = None, creds = None, vbuckets = None, data_policy = None):
        self.tenant_id = tenant_id
        self.id = id
        self.name = name
        self.creds = creds
        self.vbuckets = vbuckets
        self.data_policy = data_policy

    def get_prefix(self):
        return 'u/' + self.tenant_id

    def get_key(self):
        return self.get_prefix() + '/' + self.id

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
    def __init__(self, svci, id = None, bucket = None, obj_prefix = '', creds_id = None):
        self.svci = svci
        self.id = id
        self.bucket = bucket
        self.obj_prefix = obj_prefix
        self.creds_id = creds_id

    def get_prefix(self):
        return 'bi/' + self.tenant_id

    def get_key(self):
        return self.get_prefix() + '/' + self.id

    def encode(self):
        return {'id': self.id,
                'svci': self.svci,
                'bucket': self.bucket,
                'obj_prefix': self.obj_prefix,
                'creds_id': self.creds_id}

    def decode(self, d):
        self.id = d.get('id')
        self.svci = d.get('svci')
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

    def __init__(self, id = None, name = None, users = None, vbuckets = None):
        self.id = id
        self.name = name
        self.creds = creds
        self.users = users
        self.vbuckets = vbuckets

    def get_prefix():
        return 't/'

    def get_key(self):
        return __class__.get_prefix() + self.id

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

    def get_prefix():
        return 'svc/'

    def get_key(self):
        return __class__.get_prefix() + self.id

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

    def get_prefix():
        return 'svci/'

    def get_key(self):
        return __class__.get_prefix() + self.id

    def decode(self, d):
        self.id = d.get('id')
        self.name = d.get('name')
        self.svc_id = d.get('svc_id')
        self.buckets = decode_list(d.get('buckets'), str)
        self.cred_ids = decode_list(d.get('cred_ids'), str)
        return self

    def encode(self):
        return {'id': self.id,
                'name': self.name,
                'svc_id': self.svc_id,
                'buckets': self.buckets,
                'cred_ids': self.creds,
                }

class OCSNDataFlowEntity(OCSNEntity):
    def __init__(self, id = None, svc = None, bucket = None):
        self.id = id
        self.svc = svc
        self.bucket = bucket

    def decode(self, d):
        self.id = d.get('id')
        self.svc = d.get('svc')
        self.bucket = d.get('bucket')
        return self

    def encode(self):
        return {'id': self.id,
                'svc': self.source,
                'bucket': self.dest,
                }

class OCSNDirectionalFlow(OCSNEntity):
    def __init__(self, id = None, source = None, dest = None):
        self.id = id
        self.source = source
        self.dest = dest

    def decode(self, d):
        self.id = d.get('id')
        self.source = d.get('source')
        self.dest = d.get('dest')
        return self

    def encode(self):
        return {'id': self.id,
                'source': self.source,
                'dest': self.dest,
                }


class OCSNSymmetricFlow(OCSNEntity):
    def __init__(self, id = None, entities = None):
        self.id = id
        self.entities

    def decode(self, d):
        self.id = d.get('id')
        self.entities = decode_list(d.get('entities'), str)
        return self

    def encode(self):
        return {'id': self.id,
                'entites': self.entities,
                }


class OCSNDataFlowGroup(OCSNEntity):
    def __init__(self, id = None, directional = None, symmetric = None):
        self.directional = directional
        self.symmetric = symmetric
    
    def decode(self, d):
        self.id = d.get('id')
        self.directional = decode_list(d.get('directional'), OCSNDirectionalFlow)
        self.symmetric = decode_list(d.get('symmetric'), OCSNSymmetricFlow)
        return self

    def encode(self):
        return {'id': self.id,
                'directional': self.directional,
                'symmetric': self.symmetric,
                }

class OCSNDataFlowPolicy(OCSNEntity):
    def __init__(self, id = None, groups = None):
        self.groups = groups

    def decode(self, d):
        self.id = d.get('id')
        self.groups = decode_list(d.get('groups'), OCSNDataFlowPolicy)
        return self

    def encode(self):
        return {'id': self.id,
                'groups': self.groups,
                }


class OCSNDataFlowInstance(OCSNEntity):
    pass
