#!/usr/bin/env python3
import json
import signal
import sys
import time

import hailtop.fs as hfs

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.clickhouse import (
    ClickHouseTable,
    drop_staging_db,
)
from v03_pipeline.lib.misc.runs import get_run_ids
from v03_pipeline.lib.paths import (
    clickhouse_load_fail_file_path,
    clickhouse_load_success_file_path,
    metadata_for_run_path,
)
from v03_pipeline.lib.tasks.clickhouse_migration.constants import (
    ClickHouseMigrationType,
)

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

                    msg = f'Attempting load of run: {reference_genome.value}/{dataset_type.value}/{run_id}'
                    logger.info(msg)

                    # Run metadata
                    with hfs.open(
                        metadata_for_run_path(
                            reference_genome,
                            dataset_type,
                            run_id,
                        ),
                        'r',
                    ) as f:
                        metadata_json = json.load(f.read())
                        project_guids = metadata_json['project_guids']
                        family_guids = list(metadata_json['family_samples'].keys())
                        migration_type = (
                            ClickHouseMigrationType(metadata_json['migration_type'])
                            if 'migration_type' in metadata_json
                            else None
                        )

                    for clickhouse_table in ClickHouseTable:
                        if not clickhouse_table.should_load(
                            reference_genome,
                            dataset_type,
                            migration_type,
                        ):
                            continue
                        clickhouse_table.insert(
                            reference_genome=reference_genome,
                            dataset_type=dataset_type,
                            run_id=run_id,
                            project_guids=project_guids,
                            family_guids=family_guids,
                        )
                    with hfs.open(
                        clickhouse_load_success_file_path(
                            reference_genome,
                            dataset_type,
                            run_id,
                        ),
                        'w',
                    ) as f:
                        f.write('')
                    msg = f'Successfully loaded {reference_genome.value}/{dataset_type.value}/{run_id}'
                    logger.info(msg)
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
            reference_genome, dataset_type, run_id = None, None, None
            logger.info('Waiting for work...')
            time.sleep(SLEEP_S)


if __name__ == '__main__':
    main()
