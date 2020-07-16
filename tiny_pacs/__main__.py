# -*- coding: utf-8 -*-
import argparse

from . import config
from . import server


def main():
    args = parse_args()
    pacs_conf = config.Config()
    pacs_conf.update_config(args.config)
    if args.aet:
        pacs_conf.ae['ae_title'] = [args.aet]
    if args.port:
        pacs_conf.ae['port'] = args.port
    srv = server.Server(pacs_conf)
    srv.start_with_block()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', default=[], nargs='*',
                        help='Tiny PACS configuration')
    parser.add_argument('-a', '--aet', default=None,
                        help='Override Tiny PACS AE Title configuration')
    parser.add_argument('-p', '--port', default=None, type=int,
                        help='Override Tiny PACS port configuration')
    args = parser.parse_args()
    return args


main()
