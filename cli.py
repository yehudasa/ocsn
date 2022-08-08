import sys
import argparse
import random
import string

from ocsn.ocsn_err import *
from ocsn.service import *

import json

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
   remove                        Remove a new service
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

        svc = OCSNServiceCtl()

        result = ([ e.encode() for e in svc.list() ])

        print(json.dumps(result, indent=2))

    def _do_store(self, only_modify):

        parser = argparse.ArgumentParser(
            description='Create a service',
            usage='ocsn svc create')

        parser.add_argument('--id')
        parser.add_argument('--name')
        parser.add_argument('--region')
        parser.add_argument('--endpoint')

        args = parser.parse_args(sys.argv[3:])

        id = args.id or gen_id('svc')

        svc = OCSNService(id = id, name = args.name, region = args.region, endpoint = args.endpoint)
        svc.store(exclusive = not only_modify, only_modify = only_modify)

    def create(self):
        self._do_store(False)

    def modify(self):
        self._do_store(True)

    def remove(self):

        parser = argparse.ArgumentParser(
            description='Remove a service',
            usage='ocsn svc remove')

        parser.add_argument('--id')

        args = parser.parse_args(sys.argv[3:])

        svc = OCSNService(id = args.id)
        svc.remove()



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
   remove                        Remove a new service instance
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

        svc = OCSNServiceInstanceCtl()

        result = ([ e.encode() for e in svc.list() ])

        print(json.dumps(result, indent=2))

    def _do_store(self, only_modify):

        parser = argparse.ArgumentParser(
            description='Create a service instance',
            usage='ocsn svci create')

        parser.add_argument('--id')
        parser.add_argument('--name')
        parser.add_argument('--svc-id')
        parser.add_argument('--buckets')
        parser.add_argument('--creds')

        args = parser.parse_args(sys.argv[3:])

        buckets = split_list_arg(args.buckets)
        creds = split_list_arg(args.creds)

        id = args.id or gen_id('svci')

        svc = OCSNServiceInstance(id = id, name = args.name, args.svc_id, buckets, creds)
        svci.store(exclusive = not only_modify, only_modify = only_modify)

    def create(self):
        self._do_store(False)

    def modify(self):
        self._do_store(True)

    def remove(self):

        parser = argparse.ArgumentParser(
            description='Remove a service instance',
            usage='ocsn svci remove')

        parser.add_argument('--id')

        args = parser.parse_args(sys.argv[3:])

        svci = OCSNServiceInstance(id = args.id)
        svci.remove()



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

def main():
    cmd = OCSNCommand()._parse()
    try:
        cmd()
    except OCSNException as e:
        print('ERROR: ' + e.desc)



if __name__ == "__main__":
    main()
