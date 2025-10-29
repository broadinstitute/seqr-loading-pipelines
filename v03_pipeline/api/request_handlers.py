import json
from collections.abc import Callable
from typing import Any

import hailtop.fs as hfs
import luigi
import luigi.execution_summary

from v03_pipeline.api.model import (
    DeleteFamiliesRequest,
    LoadingPipelineRequest,
    PipelineRunnerRequest,
    RebuildGtStatsRequest,
)
from v03_pipeline.lib.core import DatasetType, FeatureFlag, ReferenceGenome
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.clickhouse import (
    delete_family_guids,
    load_complete_run,
    rebuild_gt_stats,
)
from v03_pipeline.lib.misc.retry import retry
from v03_pipeline.lib.paths import (
    clickhouse_load_success_file_path,
    metadata_for_run_path,
)
from v03_pipeline.lib.tasks.write_success_file import WriteSuccessFileTask

logger = get_logger(__name__)


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


def run_loading_pipeline(
    lpr: LoadingPipelineRequest,
    run_id: str,
    local_scheduler: bool,
    *_: Any,
):
    for attempt_id in range(3):
        luigi_task_result = luigi.build(
            [
                WriteSuccessFileTask(
                    run_id=run_id,
                    attempt_id=attempt_id,
                    **lpr.model_dump(exclude='request_type'),
                ),
            ],
            detailed_summary=True,
            local_scheduler=local_scheduler,
        )
        if luigi_task_result.status in {
            luigi.execution_summary.LuigiStatusCode.SUCCESS,
            luigi.execution_summary.LuigiStatusCode.SUCCESS_WITH_RETRY,
        }:
            break
    else:
        raise RuntimeError(luigi_task_result.status.value[1])
    if FeatureFlag.CLICKHOUSE_LOADER_DISABLED:
        project_guids, family_guids = fetch_run_metadata(
            lpr.reference_genome,
            lpr.dataset_type,
            run_id,
        )
        load_complete_run(
            lpr.reference_genome,
            lpr.dataset_type,
            run_id,
            project_guids,
            family_guids,
        )
        write_success_file(lpr.reference_genome, lpr.dataset_type, run_id)


def run_delete_families(dpr: DeleteFamiliesRequest, run_id: str, *_: Any):
    for dataset_type in DatasetType:
        for reference_genome in dataset_type.reference_genomes:
            delete_family_guids(
                reference_genome,
                dataset_type,
                run_id,
                **dpr.model_dump(exclude='request_type'),
            )


def run_rebuild_gt_stats(rgsr: RebuildGtStatsRequest, run_id: str, *_: Any):
    for dataset_type in DatasetType:
        for reference_genome in dataset_type.reference_genomes:
            rebuild_gt_stats(
                reference_genome,
                dataset_type,
                run_id,
                **rgsr.model_dump(exclude='request_type'),
            )


REQUEST_HANDLER_MAP: dict[
    type[PipelineRunnerRequest],
    Callable[[PipelineRunnerRequest, str, ...], None],
] = {
    LoadingPipelineRequest: run_loading_pipeline,
    DeleteFamiliesRequest: run_delete_families,
    RebuildGtStatsRequest: run_rebuild_gt_stats,
}
