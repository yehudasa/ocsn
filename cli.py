import sys
import argparse
import random
import string

from ocsn.ocsn_err import *
from ocsn.service import *
from ocsn.tenant import *
from ocsn.redis_client import *

import json


def dump_json(x):
    return json.dumps(x, indent=2)

def gen_id(entity_str):
    return entity_str + '-' + ''.join(random.choices(string.ascii_lowercase, k=7))

def split_list_arg(arg):
    if not arg:
        return None
    
    return arg.split(',')


class SvcCommand:
    def __init__(self, env, args):
        self.env = env
        self.args = args

    def parse(self):
        parser = argparse.ArgumentParser(
            description='OCSN control tool',
            usage='''ocsn svc <subcommand> [...]

The subcommands are:
   list                          List services
   create                        Create a new service
   modify                        Modify an existing service
   remove                        Remove a service
''')
        parser.add_argument('subcommand', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(self.args[0:1])
        if not hasattr(self, args.subcommand):
            print('Unrecognized subcommand:', args.subcommand)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        return getattr(self, args.subcommand)

    def list(self):
        parser = argparse.ArgumentParser(
            description='List services',
            usage='ocsn svc list')

        svc = OCSNServiceCtl(redis_client)

        result = ([ e.encode() for e in svc.list() ])

        print(dump_json(result))

    def _do_store(self, only_modify, desc, usage):

        parser = argparse.ArgumentParser(
            description=desc,
            usage=usage)

        id_required = only_modify

        parser.add_argument('--svc-id', required = id_required)
        parser.add_argument('--name')
        parser.add_argument('--region')
        parser.add_argument('--endpoint')

        args = parser.parse_args(sys.argv[3:])

        id = args.svc_id or gen_id('svc')

        svc = OCSNService(id = id)
        if only_modify:
            svc.load(redis_client)

        svc.apply(name = args.name, region = args.region, endpoint = args.endpoint)
        svc.store(redis_client, exclusive = not only_modify, only_modify = only_modify)

    def create(self):
        self._do_store(False, 'Create a service', 'ocsn svc create')

    def modify(self):
        self._do_store(True, 'Modify a service', 'ocsn svc modify')

    def remove(self):

        parser = argparse.ArgumentParser(
            description='Remove a service',
            usage='ocsn svc remove')

        parser.add_argument('--svc-id', required = True)

        args = parser.parse_args(sys.argv[3:])

        svc = OCSNService(id = args.svc_id)
        svc.remove(redis_client)



class SvciCommand:
    def __init__(self, env, args):
        self.env = env
        self.args = args

    def parse(self):
        parser = argparse.ArgumentParser(
            description='OCSN control tool',
            usage='''ocsn svci <subcommand> [...]

The subcommands are:
   list                          List service instances
   create                        Create a new service instance
   modify                        Modify an existing service instancce
   remove                        Remove a service instance
''')
        parser.add_argument('subcommand', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(self.args[0:1])
        if not hasattr(self, args.subcommand):
            print('Unrecognized subcommand:', args.subcommand)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        return getattr(self, args.subcommand)

    def list(self):
        parser = argparse.ArgumentParser(
            description='List service instances',
            usage='ocsn svci list')

        svci = OCSNServiceInstanceCtl(redis_client)

        result = ([ e.encode() for e in svci.list() ])

        print(dump_json(result))

    def _do_store(self, only_modify, desc, usage):

        parser = argparse.ArgumentParser(
            description=desc,
            usage=usage)

        id_required = only_modify

        parser.add_argument('--svci-id', required = id_required)
        parser.add_argument('--name')
        parser.add_argument('--svc-id')
        parser.add_argument('--buckets')
        parser.add_argument('--creds')

        args = parser.parse_args(sys.argv[3:])

        buckets = split_list_arg(args.buckets)
        creds = split_list_arg(args.creds)

        id = args.svci_id or gen_id('svci')

        svci = OCSNServiceInstance(id = id)
        if only_modify:
            svci.load(redis_client)

        svci.apply(name = args.name, svc_id = args.svc_id, buckets = buckets, creds = creds)
        svci.store(redis_client, exclusive = not only_modify, only_modify = only_modify)

    def create(self):
        self._do_store(False, 'Create a service instance', 'ocsn svci create')

    def modify(self):
        self._do_store(True, 'Modify a service instance', 'ocsn svci modify')

    def remove(self):

        parser = argparse.ArgumentParser(
            description='Remove a service instance',
            usage='ocsn svci remove')

        parser.add_argument('--svci-id', required = True)

        args = parser.parse_args(sys.argv[3:])

        svci = OCSNServiceInstance(id = args.svci_id)
        svci.remove(redis_client)


class CredsCommand:
    def __init__(self, env, args):
        self.env = env
        self.args = args

    def parse(self):
        parser = argparse.ArgumentParser(
            description='OCSN control tool',
            usage='''ocsn creds <subcommand> [...]

The subcommands are:
   list                          List credentials
   create                        Create new credentials
   modify                        Modify credentials
   remove                        Remove credentials
''')
        parser.add_argument('subcommand', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(self.args[0:1])
        if not hasattr(self, args.subcommand):
            print('Unrecognized subcommand:', args.subcommand)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        return getattr(self, args.subcommand)

    def list(self):
        parser = argparse.ArgumentParser(
            description='List credentials under specific service instance',
            usage='ocsn creds list')

        parser.add_argument('--svci-id', required = True)

        args = parser.parse_args(sys.argv[3:])

        creds = OCSNS3CredsCtl(redis_client, args.svci_id)

        result = ([ e.encode() for e in creds.list() ])

        print(dump_json(result))

    def _do_store(self, only_modify, desc, usage):

        parser = argparse.ArgumentParser(
            description=desc,
            usage=usage)

        parser.add_argument('--svci-id', required = True)
        parser.add_argument('--creds-id')
        parser.add_argument('--access-key', required = True)
        parser.add_argument('--secret', required = True)

        args = parser.parse_args(sys.argv[3:])

        id = args.creds_id or gen_id('s3creds')

        creds = OCSNS3Creds(args.svci_id, id = id)
        if only_modify:
            creds.load(redis_client)

        creds.apply(access_key = args.access_key, secret = args.secret)
        creds.store(redis_client, exclusive = not only_modify, only_modify = only_modify)

    def create(self):
        self._do_store(False, 'Create credentials', 'ocsn creds create')

    def modify(self):
        self._do_store(True, 'Modify credentials', 'ocsn creds modify')

    def remove(self):

        parser = argparse.ArgumentParser(
            description='Remove credentials',
            usage='ocsn creds remove')

        parser.add_argument('--svci-id', required = True)
        parser.add_argument('--creds-id', required = True)

        args = parser.parse_args(sys.argv[3:])

        creds = OCSNS3Creds(args.svci_id, id = args.creds_id)
        creds.remove(redis_client)


class BucketInstance:
    def __init__(self, env, args):
        self.env = env
        self.args = args

    def parse(self):
        parser = argparse.ArgumentParser(
            description='OCSN control tool',
            usage='''ocsn bi <subcommand> [...]

The subcommands are:
   list                          List bucket instances
   create                        Create a bucket instance
''')
        parser.add_argument('subcommand', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(self.args[0:1])
        if not hasattr(self, args.subcommand):
            print('Unrecognized subcommand:', args.subcommand)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        return getattr(self, args.subcommand)

    def list(self):
        parser = argparse.ArgumentParser(
            description='List bucket instances under a specific service instance',
            usage='ocsn creds list')

        parser.add_argument('--svci-id', required = True)

        args = parser.parse_args(sys.argv[3:])

        bis = OCSNBucketInstanceCtl(redis_client, args.svci_id)

        result = ([ e.encode() for e in bis.list() ])

        print(dump_json(result))

    def _do_store(self, only_modify, desc, usage):

        parser = argparse.ArgumentParser(
            description=desc,
            usage=usage)

        parser.add_argument('--svci-id', required = True)
        parser.add_argument('--bi-id')
        parser.add_argument('--bucket')
        parser.add_argument('--obj-prefix')
        parser.add_argument('--creds-id')

        args = parser.parse_args(sys.argv[3:])

        id = args.bi_id or gen_id('bi')

        bi = OCSNBucketInstance(args.svci_id, id = id)

        if only_modify:
            bi.load(redis_client)

        bi.apply(bucket = args.bucket, obj_prefix = args.obj_prefix, creds_id = args.creds_id)
        bi.store(redis_client, exclusive = not only_modify, only_modify = only_modify)

    def create(self):
        self._do_store(False, 'Create a bucket instance', 'ocsn bi create')

    def modify(self):
        self._do_store(True, 'Modify a bucket instance', 'ocsn bi modify')

    def remove(self):

        parser = argparse.ArgumentParser(
            description='Remove a bucket instance',
            usage='ocsn bi remove')

        parser.add_argument('--svci-id', required = True)
        parser.add_argument('--bi-id', required = True)

        args = parser.parse_args(sys.argv[3:])

        bi = OCSNS3Creds(args.svci, id = args.bi_id)
        bi.remove(redis_client)


class TenantCommand:
    def __init__(self, env, args):
        self.env = env
        self.args = args

    def parse(self):
        parser = argparse.ArgumentParser(
            description='OCSN control tool',
            usage='''ocsn tenant <subcommand> [...]

The subcommands are:
   list                          List tenants
   create                        Create a new tenant
   modify                        Modify an existing tenant
   remove                        Remove a tenant
''')
        parser.add_argument('subcommand', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(self.args[0:1])
        if not hasattr(self, args.subcommand):
            print('Unrecognized subcommand:', args.subcommand)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        return getattr(self, args.subcommand)

    def list(self):
        parser = argparse.ArgumentParser(
            description='List tenants',
            usage='ocsn tenant list')

        tc = OCSNTenantCtl(redis_client)

        result = ([ e.encode() for e in tc.list() ])

        print(dump_json(result))

    def _do_store(self, only_modify, desc, usage):

        parser = argparse.ArgumentParser(
            description=desc,
            usage=usage)

        id_required = only_modify

        parser.add_argument('--tenant-id', required = id_required)
        parser.add_argument('--name')

        args = parser.parse_args(sys.argv[3:])

        id = args.tenant_id or gen_id('tenant')

        tenant = OCSNTenant(id = id, name = args.name)
        if only_modify:
            tenant.load(redis_client)

        tenant.apply(name = args.name)
        tenant.store(redis_client, exclusive = not only_modify, only_modify = only_modify)

    def create(self):
        self._do_store(False, 'Create a tenant', 'ocsn tenant create')

    def modify(self):
        self._do_store(True, 'Modify a tenant', 'ocsn tenant modify')

    def remove(self):

        parser = argparse.ArgumentParser(
            description='Remove a tenant',
            usage='ocsn tenant remove')

        parser.add_argument('--tenant-id', required = True)

        args = parser.parse_args(sys.argv[3:])

        svc = OCSNTenant(id = args.tenant_id)
        svc.remove(redis_client)


class UserCommand:
    def __init__(self, env, args):
        self.env = env
        self.args = args

    def parse(self):
        parser = argparse.ArgumentParser(
            description='OCSN control tool',
            usage='''ocsn user <subcommand> [...]

The subcommands are:
   list                          List users in a tenant
   create                        Create new user
   modify                        Modify user
   remove                        Remove user 
''')
        parser.add_argument('subcommand', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(self.args[0:1])
        if not hasattr(self, args.subcommand):
            print('Unrecognized subcommand:', args.subcommand)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        return getattr(self, args.subcommand)

    def list(self):
        parser = argparse.ArgumentParser(
            description='List users in a tenant',
            usage='ocsn user list')

        parser.add_argument('--tenant-id', required = True)

        args = parser.parse_args(sys.argv[3:])

        uc = OCSNUserCtl(redis_client, args.tenant_id)

        result = ([ e.encode() for e in uc.list() ])

        print(dump_json(result))

    def _do_store(self, only_modify, desc, usage):

        parser = argparse.ArgumentParser(
            description=desc,
            usage=usage)

        parser.add_argument('--tenant-id', required = True)
        parser.add_argument('--user-id')
        parser.add_argument('--name')

        args = parser.parse_args(sys.argv[3:])

        id = args.user_id or gen_id('user')

        u = OCSNUser(args.tenant_id, id = id)
        if only_modify:
            u.load(redis_client)

        u.apply(name = args.name)
        u.store(redis_client, exclusive = not only_modify, only_modify = only_modify)

    def create(self):
        self._do_store(False, 'Create user', 'ocsn user create')

    def modify(self):
        self._do_store(True, 'Modify user', 'ocsn user modify')

    def remove(self):

        parser = argparse.ArgumentParser(
            description='Remove user',
            usage='ocsn user remove')

        parser.add_argument('--tenant-id', required = True)
        parser.add_argument('--user-id', required = True)

        args = parser.parse_args(sys.argv[3:])

        u = OCSNUser(args.tenant_id, id = args.user_id)
        u.remove(redis_client)


class VBucketCommand:
    def __init__(self, env, args):
        self.env = env
        self.args = args

    def parse(self):
        parser = argparse.ArgumentParser(
            description='OCSN control tool',
            usage='''ocsn user <subcommand> [...]

The subcommands are:
   list                          List vbuckets of a specific user
   create                        Create a new vbucket
   modify                        Modify a vbucket
   info                          Show vbucket info
   remove                        Remove a vbucket 
''')
        parser.add_argument('subcommand', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(self.args[0:1])
        if not hasattr(self, args.subcommand):
            print('Unrecognized subcommand:', args.subcommand)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        return getattr(self, args.subcommand)

    def list(self):
        parser = argparse.ArgumentParser(
            description='List users in a tenant',
            usage='ocsn user list')

        parser.add_argument('--tenant-id', required = True)
        parser.add_argument('--user-id', required = True)

        args = parser.parse_args(sys.argv[3:])

        uvb = OCSNVBucketCtl(redis_client, args.tenant_id, args.user_id)

        result = ([ e.encode() for e in uvb.list() ])

        print(dump_json(result))

    def _do_store(self, only_modify, desc, usage):

        parser = argparse.ArgumentParser(
            description=desc,
            usage=usage)

        parser.add_argument('--tenant-id', required = True)
        parser.add_argument('--user-id', required = True)
        parser.add_argument('--vbucket-id')
        parser.add_argument('--name')

        args = parser.parse_args(sys.argv[3:])

        id = args.vbucket_id or gen_id('vbucket-id')

        u = OCSNVBucket(args.tenant_id, args.user_id, id = id)
        if only_modify:
            u.load(redis_client)

        u.apply(name = args.name)
        u.store(redis_client, exclusive = not only_modify, only_modify = only_modify)

    def create(self):
        self._do_store(False, 'Create a vbucket', 'ocsn vbucket create')

    def modify(self):
        self._do_store(True, 'Modify a vbucket', 'ocsn vbucket modify')

    def info(self):

        parser = argparse.ArgumentParser(
            description='Show vbucket info',
            usage='ocsn vbucket info')

        parser.add_argument('--tenant-id', required = True)
        parser.add_argument('--user-id', required = True)
        parser.add_argument('--vbucket-id', required = True)

        args = parser.parse_args(sys.argv[3:])

        u = OCSNVBucket(args.tenant_id, args.user_id, id = args.vbucket_id)
        u.load(redis_client)

        print(dump_json(u.encode()))

    def remove(self):

        parser = argparse.ArgumentParser(
            description='Remove a vbucket',
            usage='ocsn vbucket remove')

        parser.add_argument('--tenant-id', required = True)
        parser.add_argument('--user-id', required = True)
        parser.add_argument('--vbucket-id', required = True)

        args = parser.parse_args(sys.argv[3:])

        u = OCSNVBucket(args.tenant_id, user_id = args.user_id, id = args.vbucket_id)
        u.remove(redis_client)



class OCSNCommand:

    def __init__(self):
        self.env = None

    def _parse(self):
        parser = argparse.ArgumentParser(
            description='OCSN control tool',
            usage='''ocsn <command> [<args>]

The commands are:
   svc list             List services
   svc create           Create a service
   svc modify           Modify existing service
   svc remove           Remove a service
   svci list            List service instances
   svci create          Create a service instance
   svci modify          Modify existing service instance
   svci remove          Remove a service instance
   creds list           List credentials
   creds create         Create credentials
   creds modify         Modify credentials
   creds remove         Remove credentials
   bi list              List buckets
   bi create            Create a bucket instance
   bi modify            Modify a bucket instance
   bi remove            Remove a bucket instance
   tenant list          List tenants
   tenant create        Create tenant
   tenant modify        Modify tenant
   tenant remove        Remove tenant
   user list            List users
   user create          Create a user
   user modify          Modify a user
   user remove          Remove a user
   vbucket list         List vbuckets of a specific user
   vbucket create       Create a vbucket
   vbucket modify       Modify a vbucket
   vbucket info         Show vbucket info
   vbucket remove       Remove a vbucket
''')
        parser.add_argument('command', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command) or args.command[0] == '_':
            print('Unrecognized command:', args.command)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        ret = getattr(self, args.command)
        return ret

    def svc(self):
        cmd = SvcCommand(self.env, sys.argv[2:]).parse()
        cmd()

    def svci(self):
        cmd = SvciCommand(self.env, sys.argv[2:]).parse()
        cmd()

    def creds(self):
        cmd = CredsCommand(self.env, sys.argv[2:]).parse()
        cmd()

    def bi(self):
        cmd = BucketInstance(self.env, sys.argv[2:]).parse()
        cmd()

    def tenant(self):
        cmd = TenantCommand(self.env, sys.argv[2:]).parse()
        cmd()

    def user(self):
        cmd = UserCommand(self.env, sys.argv[2:]).parse()
        cmd()

    def vbucket(self):
        cmd = VBucketCommand(self.env, sys.argv[2:]).parse()
        cmd()

def main():
    cmd = OCSNCommand()._parse()
    try:
        cmd()
    except OCSNException as e:
        print('ERROR: ' + e.desc)



if __name__ == "__main__":
    main()
