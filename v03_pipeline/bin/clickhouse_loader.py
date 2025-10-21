#!/usr/bin/env python3
import json
import signal
import sys
import time
import traceback

import hailtop.fs as hfs

from v03_pipeline.api.request_handlers import fetch_run_metadata, write_success_file
from v03_pipeline.lib.core import FeatureFlag
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.clickhouse import (
    drop_staging_db,
    load_complete_run,
)
from v03_pipeline.lib.misc.runs import get_run_ids
from v03_pipeline.lib.paths import (
    clickhouse_load_fail_file_path,
    pipeline_errors_for_run_path,
)

logger = get_logger(__name__)

SLEEP_S = 300


def signal_handler(*_):
    drop_staging_db()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def main():
    reference_genome, dataset_type, run_id, project_guids = (
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
        except Exception as e:
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
                pipeline_errors_json = {
                    'project_guids': project_guids,
                    'error_messages': ['Failed during ClickHouse Load'],
                    'traceback': {
                        'error': str(e),
                        'type': type(e).__name__,
                        'traceback': traceback.format_exc(),
                    },
                }
                with hfs.open(
                    pipeline_errors_for_run_path(
                        reference_genome,
                        dataset_type,
                        run_id,
                    ),
                    'w',
                ) as f:
                    json.dump(pipeline_errors_json, f)
        finally:
            reference_genome, dataset_type, run_id = None, None, None
            logger.info('Waiting for work...')
            time.sleep(SLEEP_S)


if __name__ == '__main__':
    main()
