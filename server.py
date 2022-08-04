from flask import Flask, request

from .ocsn_types import *

app = Flask(__name__)
app.json_encoder = OCSNEntityJSONEncoder

@app.route('/')
def index():
    return 'index!'

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

@app.route('/svc/<service>', methods = ['GET', 'POST', 'DELETE'])
def svc_handler(service):
    #GET
    if request.method == 'GET':
        svc = OCSNService(id = service).load()
        return svc.encode_json()

    # POST
    data = request.get_data()
    svc = OCSNService(id = service)
    svc = svc.decode_json(data)
    if svc:
        svc.id = service # force provided id
        svc.store()

    return ''

@app.route('/svci/<id>', methods = ['GET', 'POST', 'DELETE'])
def svci_handler(id):
    #GET
    if request.method == 'GET':
        svci = OCSNServiceInstance(id = id).load()
        return svci.encode_json()

    # POST
    data = request.get_data()
    svci = OCSNServiceInstance(id = id)
    svci = svci.decode_json(data)
    if svci:
        svci.id = id # force provided id
        svci.store()

    return ''

