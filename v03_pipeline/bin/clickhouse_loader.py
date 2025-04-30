#!/usr/bin/env python3
import signal
import sys
import time

import clickhouse_connect
from clickhouse_connect import common

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.retry import retry
from v03_pipeline.lib.misc.runs import get_run_ids
from v03_pipeline.lib.model.environment import Env

logger = get_logger(__name__)

LIVE_CLICKHOUSE_DATABASE = 'seqr'
STAGING_CLICKHOUSE_DATABASE = 'staging'


def signal_handler(*_):
    client = get_clickhouse_client()
    drop_staging_tables(client)
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def get_clickhouse_client() -> clickhouse_connect.driver.client.Client:
    # per the docs, require a new session id for every query
    common.set_setting('autogenerate_session_id', False)
    return clickhouse_connect.get_client(
        host=Env.CLICKHOUSE_SERVICE_HOSTNAME,
        port=Env.CLICKHOUSE_SERVICE_PORT,
        username=Env.CLICKHOUSE_USER,
        password=Env.CLICKHOUSE_PASSWORD,
    )


@retry(tries=3, delay=5)
def drop_staging_tables(client):
    logger.info('Dropping all staging tables')
    client.command(f'DROP DATABASE IF EXISTS {STAGING_CLICKHOUSE_DATABASE};')


def main():
    while True:
        try:
            successful_pipeline_runs, _ = get_run_ids()
            for reference_genome, dataset_type in successful_pipeline_runs:
                num_successful_runs = len(
                    successful_pipeline_runs[(reference_genome, dataset_type)],
                )
                logger.info(
                    f'{reference_genome.value}/{dataset_type.value} has {num_successful_runs} successful runs',
                )
            client = get_clickhouse_client()
            result = client.query('SELECT now(), version()')
            rows = result.result_rows
            logger.info(
                f'Successfully connected to Clickhouse: {rows[0][0]}, {rows[0][1]}',
            )
        except Exception:
            logger.exception('Unhandled Exception')
        finally:
            time.sleep(5)


if __name__ == '__main__':
    main()
