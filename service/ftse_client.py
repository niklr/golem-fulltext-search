#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
from rpyc.utils.factory import unix_connect

IN_PATH = Path("/golem/in").absolute()
OUT_PATH = Path("/golem/out").absolute()

def test():
    print("test")


def init():
    c.init_index(IN_PATH.as_posix(), OUT_PATH.as_posix())
    print("init success")


def search(term: str):
    result = c.search(term)
    print(result)


def dump(filename: str):
    content = c.read_file(os.path.join(IN_PATH, filename))
    print(content)


def get_arg_parser():
    parser = argparse.ArgumentParser(description="ftse service")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--test", action="store_true")
    group.add_argument("--init", action="store_true")
    group.add_argument("--search", help="Provide a term to be searched for")
    group.add_argument("--dump", help="Provide a filename to be dumped")
    return parser


if __name__ == "__main__":
    conn = unix_connect('/golem/run/uds_socket')
    conn._config['sync_request_timeout'] = None
    c = conn.root

    arg_parser = get_arg_parser()
    args = arg_parser.parse_args()

    if args.test:
        test()
    elif args.init:
        init()
    elif args.search:
        search(args.search)
    elif args.dump:
        dump(args.dump)