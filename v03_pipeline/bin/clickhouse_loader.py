#!/usr/bin/env python3
import signal
import sys
import time

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.clickhouse import (
    ClickhouseTable,
    direct_insert,
    get_clickhouse_client,
)
from v03_pipeline.lib.misc.retry import retry
from v03_pipeline.lib.misc.runs import get_run_ids

logger = get_logger(__name__)

STAGING_CLICKHOUSE_DATABASE = 'staging'
SLEEP_S = 10


def signal_handler(*_):
    drop_staging_tables()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


@retry(tries=3, delay=5)
def drop_staging_tables():
    logger.info('Dropping all staging tables')
    client = get_clickhouse_client()
    client.command(f'DROP DATABASE IF EXISTS {STAGING_CLICKHOUSE_DATABASE};')


def main():
    while True:
        try:
            successful_pipeline_runs, successful_clickhouse_loads = get_run_ids()
            for (
                reference_genome,
                dataset_type,
            ), run_ids in successful_pipeline_runs.items():
                for run_id in run_ids:
                    if (
                        run_id
                        in successful_clickhouse_loads[reference_genome, dataset_type]
                    ):
                        continue

                for clickhouse_table in ClickhouseTable:
                    if not clickhouse_table.should_load(reference_genome, dataset_type):
                        continue
                    direct_insert(
                        reference_genome,
                        dataset_type,
                        run_id,
                        clickhouse_table,
                    )

        except Exception:
            logger.exception('Unhandled Exception')
        finally:
            time.sleep(SLEEP_S)


if __name__ == '__main__':
    main()
