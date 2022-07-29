import etcd3
from .ocsn_err import OCSNException, OCSNError
 


class EtcdClient:
    def __init__(self):
        self.etcd = etcd3.client()

    def get(self, key):
        val, meta = self.etcd.get(key)

        print(key)

        if not val:
            raise OCSNException(OCSNError.NOT_FOUND, 'key {} not found'.format(key))

        return meta.key.decode('utf-8'), val.decode('utf-8')
            
    def put(self, key, data):
        print('etcd put: key={k} data={d}'.format(k=key, d=data))
        self.etcd.put(key, data)

