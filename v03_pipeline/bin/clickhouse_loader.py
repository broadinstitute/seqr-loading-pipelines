#!/usr/bin/env python3
import signal
import sys
import time

import hailtop.fs as hfs

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.clickhouse import (
    ClickHouseTable,
    drop_staging_db,
    insert,
)
from v03_pipeline.lib.misc.runs import get_run_ids
from v03_pipeline.lib.paths import clickhouse_load_fail_file_path

logger = get_logger(__name__)

SLEEP_S = 10


def signal_handler(*_):
    drop_staging_db()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def main():
    reference_genome, dataset_type, run_id = None, None, None
    while True:
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

                for clickhouse_table in ClickHouseTable:
                    if not clickhouse_table.should_load(reference_genome, dataset_type):
                        continue
                    insert(
                        reference_genome,
                        dataset_type,
                        run_id,
                        clickhouse_table,
                    )

        except Exception:
            logger.exception('Unhandled Exception')
            if reference_genome and dataset_type and run_id:
                with hfs.open(
                    clickhouse_load_fail_file_path(
                        reference_genome,
                        dataset_type,
                        run_id,
                    ),
                    'w',
                ) as f:
                    f.write('')
        finally:
            time.sleep(SLEEP_S)


if __name__ == '__main__':
    main()
