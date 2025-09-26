from collections.abc import Callable
from typing import Any

import luigi
import luigi.execution_summary

from v03_pipeline.api.model import (
    DeleteFamiliesRequest,
    LoadingPipelineRequest,
    PipelineRunnerRequest,
)
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.clickhouse import (
    delete_family_guids,
)
from v03_pipeline.lib.model import DatasetType
from v03_pipeline.lib.tasks.write_success_file import WriteSuccessFileTask

logger = get_logger(__name__)


def run_loading_pipeline(
    lpr: LoadingPipelineRequest,
    run_id: str,
    local_scheduler: bool,
    **_: Any,
):
    for attempt_id in range(3):
        luigi_task_result = luigi.build(
            [
                WriteSuccessFileTask(
                    run_id=run_id,
                    attempt_id=attempt_id,
                    **lpr.model_dump(),
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


def run_delete_families(dpr: DeleteFamiliesRequest, run_id: str, **_: Any):
    for dataset_type in DatasetType:
        for reference_genome in dataset_type.reference_genomes:
            delete_family_guids(
                reference_genome,
                dataset_type,
                run_id,
                **dpr.model_dump(),
            )


REQUEST_HANDLER_MAP: dict[
    type[PipelineRunnerRequest],
    Callable[[PipelineRunnerRequest, str, ...], None],
] = {
    LoadingPipelineRequest: run_loading_pipeline,
    DeleteFamiliesRequest: run_delete_families,
}
