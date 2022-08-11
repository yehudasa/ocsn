from .ocsn_types import OCSNService, OCSNServiceInstance, OCSNS3Creds
from .redis_client import *


class OCSNServiceCtl:
    def __init__(self, client):
        self.client = client

    def list(self):
        for item in self.client.list(OCSNService.get_prefix()):
            yield OCSNService().decode_json(item)

class OCSNServiceInstanceCtl:
    def __init__(self):
        pass

    def list(self):
        for item in self.client.list(OCSNServiceInstance.get_prefix()):
            yield OCSNServiceInstance().decode_json(item)

class OCSNS3CredsCtl:
    def __init__(self, client, svci):
        self.client = client
        self.svci = svci

    def list(self):
        creds = OCSNS3Creds(self.svci)
        prefix = creds.get_prefix()
        for item in self.client.list(prefix):
            yield creds.decode_json(item)

class OCSNBucketInstanceCtl:
    def __init__(self, client, svci):
        self.client = client
        self.svci = svci

    def list(self):
        bi = OCSNBucketInstance(self.svci)
        prefix = bi.get_prefix()
        for item in self.client.list(prefix):
            yield bi.decode_json(item)

