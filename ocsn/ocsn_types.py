from abc import abstractmethod

import random
import string
import copy
import itertools

from flask import json
from flask.json import JSONEncoder



def safestr(s):
    if not s:
        return ''
    return s

def safecmp(s1, s2):
    return (s1 or '') == (s2 or '')

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
        result[k] = T._decode(None, v)

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

    def _decode(obj, d):
        if not obj:
            obj = OCSNBucketInstanceID()
        obj.bi_id = d.get('bi')
        obj.svci_id = d.get('svci')
        return obj

    def decode(self, d):
        return __class__._decode(self, d)


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
        self.bis = decode_dict(d.get('bis'), OCSNBucketInstanceID)
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

    def get_prefix_opt(self):
        prefix = 'b/'
        if self.tenant_id:
            prefix += self.tenant_id + '/'
            if self.user_id:
                prefix += self.user_id + '/'
        return prefix
   
    def get_prefix(self):
        return 'b/' + self.tenant_id + '/' + self.user_id + '/'

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
        mappings = self.mappings
        if mappings:
            mappings = self.mappings.encode()
        return {'id': self.id,
                'name': self.name,
                'mappings': mappings }

    def decode(self, d):
        self.id = d.get('id')
        self.user = d.get('name')
        self.mappings = OCSNBucketInstanceMapping().decode(d.get('mappings'))
        return self

class OCSNTenantPolicy(OCSNEntity):
    def __init__(self):
        self.svc_id = None

    def decode(self, d):
        if not d:
            return OCSNTenantPolicy()

        self.svc_id = d.get('svc_id')
        return self

    def encode(self):
        return {'svc_id': self.svc_id, }

    def check(self, svc_id):
        if not self.svc_id:
            return True

        return (self.svc_id == svc_id)

class OCSNTenant(OCSNEntity):

    def __init__(self, id = None, name = None, users = None, vbuckets = None, policy = None):
        self.id = id
        self.name = name
        self.users = users
        self.vbuckets = vbuckets
        self.policy = policy

    def apply(self, name = None, users = None, vbuckets = None, policy = None):
        if name:
            self.name = name
        if users:
            self.users = users
        if vbuckets:
            self.vbuckets = vbuckets
        if policy:
            self.policy = policy

    def get_prefix():
        return 't/'

    def get_key(self):
        return __class__.get_prefix() + self.id

    def decode(self, d):
        self.id = d.get('id')
        self.name = d.get('name')
        self.users = decode_list(d.get('users'), OCSNUser)
        self.vbuckets = decode_list(d.get('vbuckets'), OCSNVBucket)
        self.policy = OCSNTenantPolicy().decode(d.get('policy'))
        return self

    def encode(self):
        return {'id': self.id,
                'name': self.name,
                'users': self.users,
                'vbuckets': self.vbuckets,
                'policy': self.policy.encode(),
                }

    def check_policy(self, svc_id):
        return self.policy.check(svc_id)


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
    def __init__(self, svc_id = None, bucket = None, obj_prefix = None):
        self.svc_id = svc_id
        self.bucket = bucket
        self.obj_prefix = obj_prefix

    def decode(self, d):
        self.svc_id = d.get('svc_id')
        self.bucket = d.get('bucket')
        self.obj_prefix = d.get('obj_prefix')
        return self

    def encode(self):
        return {'svc_id': self.svc_id,
                'bucket': self.bucket,
                'obj_prefix': self.obj_prefix,
                }

    def apply(self, entity):
        new_entity = OCSNDataFlowEntity(self.svc_id, self.bucket, self.obj_prefix)

        if not self.bucket:
            new_entity.bucket = entity.bucket
        else:
            new_entity.bucket.replace('*', entity.bucket)

        return new_entity

    def compare(self, entity):
        return safecmp(self.svc_id, entity.svc_id) and safecmp(self.bucket, entity.bucket) and safecmp(self.obj_prefix, entity.obj_prefix)



class OCSNDirectionalFlow(OCSNEntity):
    def __init__(self, source = None, dest = None):
        self.source = source
        self.dest = dest

    def _decode(obj, d):
        if not obj:
            obj = OCSNDirectionalFlow()
        obj.source = OCSNDataFlowEntity().decode(d.get('source'))
        obj.dest = OCSNDataFlowEntity().decode(d.get('dest'))
        return obj

    def decode(self, d):
        return OCSNDirectionalFlow._decode(self, d)

    def encode(self):
        return {'source': self.source.encode(),
                'dest': self.dest.encode(),
                }

    def check(self, source, dest):
        s = self.source.apply(source)
        d = self.dest.apply(source) # use the source bucket in case of wildcard
        ret = s.compare(source) and d.compare(dest)
        return ret


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
    def __init__(self, id = None):
        self.id = id
        self.flows = None

    def apply(self, flows = None):
        if flows:
            self.flows = flows

    def get_prefix():
        return 'dataflow/'

    def get_key(self):
        return __class__.get_prefix() + self.id

    def decode(self, d):
        self.id = d.get('id')
        self.flows = decode_dict(d.get('flows'), OCSNDirectionalFlow)
        return self

    def encode(self):
        d = {}
        for k, v in self.flows.items():
            d[k] = v.encode()

        return {'id': self.id,
                'flows': d,
                }

    def append(self, flow, flow_id = None):
        if not flow_id:
            flow_id = self.id + '/' + ''.join(random.choices(string.ascii_lowercase, k=5))

        if not self.flows:
            self.flows = {}

        self.flows[flow_id] = flow

    def pop(self, flow_id):
        try:
            self.flows.pop(flow_id)
        except:
            pass

        return bool(self.flows)

    def check(self, source, dest):
        for _, f in self.flows.items():
            if f.check(source, dest):
                return True
        return False

