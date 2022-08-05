import sys
import argparse
import random
import string

from ocsn.ocsn_err import *
from ocsn.service import *

import json

def gen_id():
    return 'svc-' + ''.join(random.choices(string.ascii_lowercase, k=7))

class SvcCommand:
    def __init__(self, env, args):
        self.env = env
        self.args = args

    def parse(self):
        parser = argparse.ArgumentParser(
            description='OCSN control tool',
            usage='''ocsn svc <subcommand> [--enable[=<true|<false>]]

The subcommands are:
   list                          List services
   create                        Create a new service
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

    def create(self):
        parser = argparse.ArgumentParser(
            description='Create a service',
            usage='ocsn svc create')

        parser.add_argument('--id')
        parser.add_argument('--name')
        parser.add_argument('--region')
        parser.add_argument('--endpoint')

        args = parser.parse_args(sys.argv[3:])

        id = args.id or gen_id()

        svc = OCSNService(id = id, name = args.name, region = args.region, endpoint = args.endpoint)
        svc.store()





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
