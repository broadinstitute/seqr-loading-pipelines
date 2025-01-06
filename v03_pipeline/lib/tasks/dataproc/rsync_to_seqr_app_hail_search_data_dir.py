import os

import luigi

from v03_pipeline.lib.model import Env
from v03_pipeline.lib.paths import pipeline_prefix_path, v03_reference_dataset_prefix
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDatasetQuery
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)


def rsync_command(src_path: str, dst_path: str) -> list[str]:
    return [
        '/bin/bash',
        '-cx',
        f'mkdir -p {dst_path} && gsutil -qm rsync -rd -x .*runs.* {src_path} {dst_path} && sync {Env.SEQR_APP_HAIL_SEARCH_DATA_DIR} && sync {Env.SEQR_APP_REFERENCE_DATASETS_DIR}',
    ]


@luigi.util.inherits(BaseLoadingRunParams)
class RsyncToSeqrAppHailSearchDataDir(luigi.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done = False

    def output(self) -> None:
        return None

    def complete(self) -> bool:
        return self.done

    def run(self) -> None:
        if not (
            Env.SEQR_APP_HAIL_SEARCH_DATA_DIR and Env.SEQR_APP_REFERENCE_DATASETS_DIR
        ):
            self.done = True
            return
        cmds = []

        # Sync Pipeline Tables
        src_path = pipeline_prefix_path(
            Env.HAIL_SEARCH_DATA_DIR,
            self.reference_genome,
            self.dataset_type,
        )
        dst_path = pipeline_prefix_path(
            Env.SEQR_APP_HAIL_SEARCH_DATA_DIR,
            self.reference_genome,
            self.dataset_type.hail_search_value,
        )
        cmds.append(
            rsync_command(src_path, dst_path),
        )

        # Sync RDQs
        for query in ReferenceDatasetQuery.for_reference_genome_dataset_type(
            self.reference_genome,
            self.dataset_type,
        ):
            src_path = os.path.join(
                v03_reference_dataset_prefix(
                    Env.REFERENCE_DATASETS_DIR,
                    self.reference_genome,
                    self.dataset_type.hail_search_value,
                ),
                f'{query.value}.ht',
            )
            dst_path = os.path.join(
                v03_reference_dataset_prefix(
                    Env.SEQR_APP_REFERENCE_DATASETS_DIR,
                    self.reference_genome,
                    self.dataset_type.hail_search_value,
                ),
                f'{query.value}.ht',
            )
            cmds.append(
                rsync_command(src_path, dst_path),
            )

        self.done = True
