import os
import subprocess

import luigi

from v03_pipeline.lib.model import Env
from v03_pipeline.lib.paths import pipeline_prefix, valid_reference_dataset_query_path
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDatasetQuery
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)


def hail_search_value(value: str) -> str:
    return value.replace('SV', 'SV_WGS').replace(
        'GCNV',
        'SV_WES',
    )


def rsync_command(src_path: str, dst_path: str) -> list[str]:
    return [
        '/bin/bash',
        '-cx',
        f'mkdir -p {dst_path} && gsutil -qm rsync -rd -x .*runs.* {src_path} {dst_path} && sync {dst_path}',
    ]


@luigi.util.inherits(BaseLoadingRunParams)
class RsyncToSeqrAppDirsTask(luigi.Task):
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

        if not (
            Env.HAIL_SEARCH_DATA_DIR.startswith('gs://')
            and Env.REFERENCE_DATASETS_DIR.startswith('gs://')
        ):
            msg = 'Overridden HAIL_SEARCH_DATA_DIR and REFERENCE_DATASETS_DIR must be Google Cloud buckets.'
            raise RuntimeError(msg)

        # Sync Pipeline Tables
        src_path = pipeline_prefix(
            Env.HAIL_SEARCH_DATA_DIR,
            self.reference_genome,
            self.dataset_type,
        )
        dst_path = hail_search_value(
            pipeline_prefix(
                Env.SEQR_APP_HAIL_SEARCH_DATA_DIR,
                self.reference_genome,
                self.dataset_type,
            ),
        )
        subprocess.call(
            rsync_command(src_path, dst_path),  # noqa: S603
        )

        # Sync RDQs
        for query in ReferenceDatasetQuery.for_reference_genome_dataset_type(
            self.reference_genome,
            self.dataset_type,
        ):
            src_path = valid_reference_dataset_query_path(
                self.reference_genome,
                self.dataset_type,
                query,
            )
            dst_path = os.path.join(
                hail_search_value(
                    valid_reference_dataset_query_path(
                        self.reference_genome,
                        self.dataset_type,
                        query,
                        Env.SEQR_APP_REFERENCE_DATASETS_DIR,
                    ),
                ),
            )
            subprocess.call(
                rsync_command(src_path, dst_path),  # noqa: S603
            )
        self.done = True
