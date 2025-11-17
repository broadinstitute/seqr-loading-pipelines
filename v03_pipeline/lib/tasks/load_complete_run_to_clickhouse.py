import json

import hailtop.fs as hfs
import luigi
import luigi.util

from v03_pipeline.lib.misc.clickhouse import (
    ClickHouseTable,
    TableNameBuilder,
    load_complete_run,
    logged_query,
)
from v03_pipeline.lib.paths import metadata_for_run_path
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.write_success_file import WriteSuccessFileTask


@luigi.util.inherits(BaseLoadingRunParams)
class LoadCompleteRunToClickhouse(luigi.Task):
    attempt_id = luigi.IntParameter()

    def requires(self) -> luigi.Task:
        return [self.clone(WriteSuccessFileTask)]

    def complete(self):
        table_name_builder = TableNameBuilder(
            self.reference_genome,
            self.dataset_type,
            self.run_id,
        )
        max_key_src = logged_query(
            f"""
            SELECT max(key) FROM {table_name_builder.src_table(ClickHouseTable.ANNOTATIONS_MEMORY)}
            """,
        )[0][0]
        return logged_query(
            f"""
            SELECT EXISTS (
                SELECT 1
                FROM {table_name_builder.dst_table(ClickHouseTable.ANNOTATIONS_MEMORY)}
                WHERE key = %(max_key_src)
            );
            """,
            {'max_key_src': max_key_src},
        )[0][0]

    def run(self):
        with hfs.open(
            metadata_for_run_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        ) as f:
            family_guids = list(json.load(f)['family_samples'].keys())
        load_complete_run(
            self.reference_genome,
            self.dataset_type,
            self.run_id,
            self.project_guids,
            family_guids,
        )
