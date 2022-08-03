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

@app.route('/user/<tenant>/<username>', methods = ['GET', 'POST', 'DELETE'])
def user_handler(username):
    #GET
    if request.method == 'GET':
        u = OCSNUser(tenant = tenant, id = username).load()
        return u.encode_json()

    # POST
    data = request.get_data()
    u = OCSNUser(tenant = tenant, id = username)
    u = u.decode_json(data)
    if u:
        u.tenant = tenant # forcce provided tenant
        u.id = username # force provided id
        u.store()

    return ''

@app.route('/tenant/<tenant>', methods = ['GET', 'POST', 'DELETE'])
def user_handler(tenant):
    #GET
    if request.method == 'GET':
        u = OCSNTenant(id = tenant).load()
        return u.encode_json()

    # POST
    data = request.get_data()
    u = OCSNTenant(id = tenant)
    u = u.decode_json(data)
    if u:
        u.id = tenant # force provided id
        u.store()

    return ''
