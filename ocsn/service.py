from .ocsn_types import OCSNService, OCSNServiceInstance, OCSNS3Creds
from .redis_client import *


class OCSNServiceCtl:
    def __init__(self):
        pass

    def list(self):
        for item in redis_client.list(OCSNService.get_prefix()):
            yield OCSNService().decode_json(item)

