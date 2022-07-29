from flask import Flask, request

from .ocsn_types import *
from .etcd_client import *

import etcd3

app = Flask(__name__)
app.json_encoder = OCSNEntityJSONEncoder

etcd_client = EtcdClient()

@app.route('/')
def index():
    return 'index!'

#adding variables
@app.route('/user/<username>', methods = ['GET', 'POST', 'DELETE'])
def user_handler(username):
    #GET
    if request.method == 'GET':
        u = OCSNUser(id = username).load()
        return u.encode_json()

    # POST
    data = request.get_data()
    u = OCSNUser(id = username)
    u = u.decode_json(data)
    if u:
        u.id = username # force provided id
        u.store()

    return ''
