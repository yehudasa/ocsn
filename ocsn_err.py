from enum import Enum

class OCSNError(Enum):
    ERROR = 1
    NOT_FOUND = 2

class OCSNException(Exception):
    def __init__(self, err, desc = None):
        self.err = err
        self.desc = desc
        

