#!/usr/bin/env python3
import argparse
import signal
import sys

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.clickhouse import (
    delete_family_guids,
    drop_staging_db,
)

logger = get_logger(__name__)

SLEEP_S = 300


def signal_handler(*_):
    drop_staging_db()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project-guid',
        required=True,
        help='The GUID for the project (string).',
    )
    parser.add_argument(
        '--family-guids',
        required=True,
        type=lambda s: s.split(','),
        help='Comma-separated list of family GUIDs (e.g. F016885_au_21200,F016886_au_21201).',
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    delete_family_guids(args.project_guid, args.family_guids)
