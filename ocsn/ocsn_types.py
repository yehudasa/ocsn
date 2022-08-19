from abc import abstractmethod

from flask import json
from flask.json import JSONEncoder





def decode_list(d, T):
    if d is None:
        return None

    l = []
    for item in d:
        l.append(T.decode(item))

    return l

def decode_dict(d, T):
    if d is None:
        return None

    result = {}
    for k, v in d.items():
        result[k] = T.decode(v)

    return result


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

    def load(self, client):
        v = client.get(self.get_key())
        return self.decode_json(v)

    def store(self, client, exclusive = None, only_modify = None):
        k = self.get_key()
        client.put(k, self.encode_json(), exclusive = exclusive, only_modify = only_modify)

    def remove(self, client):
        k = self.get_key()
        client.remove(k)



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

    def apply(self, access_key = None, secret = None):
        if access_key:
            self.access_key = access_key
        if secret:
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

    def apply(self, name = None, creds = None, vbuckets = None, data_policy = None):
        if name:
            self.name = name
        if creds:
            self.creds = creds
        if vbuckets:
            self.vbuckets = vbuckets
        if data_policy:
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

    def apply(self, bucket = None, obj_prefix = None, creds_id = None):
        if bucket:
            self.bucket = bucket
        if obj_prefix:
            self.obj_prefix = obj_prefix
        if creds_id:
            self.creds_id = creds_id

    def get_prefix(self):
        return 'bi/' + self.svci

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

class OCSNBucketInstanceID(OCSNEntity):
    def __init__(self, svci_id = None, bi_id = None):
        self.svci_id = svci_id
        self.bi_id = bi_id

    def encode(self):
        return {'bi': self.bi_id,
                'svci': self.svci_id,
                }

    def decode(self, d):
        self.bi_id = d.get('bi')
        self.svci_id = d.get('svci')
        return self


class OCSNBucketInstanceMapping(OCSNEntity):
    def __init__(self):
        self.bis = None # bucket instances


    def insert(self, entry_id, bi):
        if not self.bis:
            self.bis = {}

        self.bis[entry_id] = OCSNBucketInstanceID(bi.svci, bi.id)

    def remove(self, entry_id):
        if not self.bis:
            return

        try:
            self.bis.pop(entry_id)
        except:
            pass

    def encode(self):
        d = {}
        for k, v in self.bis.items():
            d[k] = v.encode()

        return { 'bis': d }

    def decode(self, d):
        if not d:
            return None

        self.id = d.get('id')
        self.bis = decode_dict(d.get('bis'), OCSNBucketInstanceID())
        # self.data_policy = OCSNDataPolicy().decode(d.get('data_policy'))
        return self


class OCSNVBucket(OCSNEntity):

    def __init__(self, tenant_id, user_id, id = None, name = None, mappings = None):
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.id = id
        self.name = name
        self.mappings = mappings

    def apply(self, name = None, mappings = None):
        if name:
            self.name = name
        if mappings:
            self.mappings = mappings
   
    def get_prefix(self):
        return 'b/' + self.tenant_id + '/' + self.user_id

    def get_key(self):
        return self.get_prefix() + self.id

    def map(self, entry_id, bi):
        if not self.mappings:
            self.mappings = OCSNBucketInstanceMapping()

        self.mappings.insert(entry_id, bi)
    
    def unmap(self, entry_id):
        if not self.mappings:
            return

        self.mappings.remove(entry_id)

    def encode(self):
        return {'id': self.id,
                'name': self.name,
                'mappings': self.mappings.encode() }

    def decode(self, d):
        self.id = d.get('id')
        self.user = d.get('name')
        self.mappings = OCSNBucketInstanceMapping().decode(d.get('mappings'))
        return self


class OCSNTenant(OCSNEntity):

    def __init__(self, id = None, name = None, users = None, vbuckets = None):
        self.id = id
        self.name = name
        self.users = users
        self.vbuckets = vbuckets

    def apply(self, name = None, users = None, vbuckets = None):
        if name:
            self.name = name
        if users:
            self.users = users
        if vbuckets:
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

    def apply(self, name = None, region = None, endpoint = None):
        if name:
            self.name = name
        if region:
            self.region = region
        if endpoint:
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

    def apply(self, name = None, svc_id = None, buckets = None, creds = None):
        if name:
            self.name = name
        if svc_id:
            self.svc_id = svc_id
        if buckets:
            self.buckets = buckets
        if creds:
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
