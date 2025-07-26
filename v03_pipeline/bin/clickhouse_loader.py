#!/usr/bin/env python3
import json
import signal
import sys
import time

import hailtop.fs as hfs

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.clickhouse import (
    drop_staging_db,
    load_complete_run,
)
from v03_pipeline.lib.misc.retry import retry
from v03_pipeline.lib.misc.runs import get_run_ids
from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.paths import (
    clickhouse_load_fail_file_path,
    clickhouse_load_success_file_path,
    metadata_for_run_path,
)

logger = get_logger(__name__)

SLEEP_S = 10


def signal_handler(*_):
    drop_staging_db()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


@retry()
def fetch_run_metadata(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
) -> tuple[list[str], list[str]]:
    # Run metadata
    with hfs.open(
        metadata_for_run_path(
            reference_genome,
            dataset_type,
            run_id,
        ),
        'r',
    ) as f:
        metadata_json = json.load(f)
        project_guids = metadata_json['project_guids']
        family_guids = list(metadata_json['family_samples'].keys())
    return project_guids, family_guids


@retry()
def write_success_file(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
):
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
                    project_guids, family_guids = fetch_run_metadata(
                        reference_genome,
                        dataset_type,
                        run_id,
                    )
                    load_complete_run(
                        reference_genome,
                        dataset_type,
                        run_id,
                        project_guids,
                        family_guids,
                    )
                    write_success_file(reference_genome, dataset_type, run_id)
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
