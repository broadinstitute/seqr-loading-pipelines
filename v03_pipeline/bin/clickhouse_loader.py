#!/usr/bin/env python3
import signal
import sys
import time

from v03_pipeline.api.request_handlers import clickhouse_load_run
from v03_pipeline.lib.core import FeatureFlag
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.clickhouse import (
    drop_staging_db,
)
from v03_pipeline.lib.misc.runs import get_run_ids

logger = get_logger(__name__)

SLEEP_S = 300


def signal_handler(*_):
    drop_staging_db()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def main():
    reference_genome, dataset_type, run_id = (
        None,
        None,
        None,
        None,
    )
    while True:
        if FeatureFlag.CLICKHOUSE_LOADER_DISABLED:
            logger.info('Waiting for work...')
            time.sleep(SLEEP_S)
            continue
        try:
            (
                successful_pipeline_runs,
                successful_clickhouse_loads,
                failed_clickhouse_loads,
            ) = get_run_ids()
            for (
                reference_genome,
                dataset_type,
            ), run_ids in successful_pipeline_runs.items():
                for run_id in run_ids:
                    if (
                        run_id
                        in successful_clickhouse_loads[reference_genome, dataset_type]
                    ) or (
                        run_id
                        in failed_clickhouse_loads[reference_genome, dataset_type]
                    ):
                        continue
                    clickhouse_load_run(reference_genome, dataset_type, run_id)
        except Exception:
            logger.exception('Unhandled Exception')
        finally:
            reference_genome, dataset_type, run_id = None, None, None
            logger.info('Waiting for work...')
            time.sleep(SLEEP_S)


if __name__ == '__main__':
    main()
