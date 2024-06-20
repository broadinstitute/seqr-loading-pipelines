import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.paths import family_table_path
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.update_project_table import (
    UpdateProjectTableTask,
)


@luigi.util.inherits(BaseLoadingRunParams)
class WriteFamilyTableTask(BaseWriteTask):
    project_guid = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()
    family_guid = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            family_table_path(
                self.reference_genome,
                self.dataset_type,
                self.family_guid,
            ),
        )

    def complete(self) -> bool:
        return (
            not self.force
            and super().complete()
            and hl.eval(
                hl.read_table(self.output().path).updates.contains(self.callset_path),
            )
        )

    def requires(self) -> luigi.Task:
        return self.clone(UpdateProjectTableTask, force=False)

    def create_table(self) -> hl.Table:
        project_ht = hl.read_table(self.input().path)
        family_i = project_ht.globals.family_guids.index(self.family_guid)
        ht = project_ht.transmute(
            entries=project_ht.family_entries[family_i],
        )
        ht = ht.filter(hl.is_defined(ht.entries))
        return ht.select_globals(
            sample_ids=ht.family_samples[self.family_guid],
            sample_type=self.sample_type.value,
            updates={self.callset_path},
        )
