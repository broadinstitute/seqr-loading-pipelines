import hail as hl
import luigi

from v03_pipeline.lib.definitions import DatasetType, ReferenceGenome
from v03_pipeline.lib.misc.io import write_ht
from v03_pipeline.lib.paths import variant_annotations_table_path
from v03_pipeline.lib.tasks.base.base_pipeline_task import BasePipelineTask
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, GCSorLocalTarget


def empty_variant_annotations_table(
    dataset_type: DatasetType,
    reference_genome: ReferenceGenome,
) -> hl.Table:
    key_type = dataset_type.variant_annotations_table_key_type(reference_genome)
    return hl.Table.parallelize(
        [],
        key_type,
        key=key_type.fields,
    )


class BaseVariantAnnotationsTableTask(BasePipelineTask):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            variant_annotations_table_path(
                self.env,
                self.reference_genome,
                self.dataset_type,
            ),
        )

    def complete(self) -> bool:
        return GCSorLocalFolderTarget(self.output().path).exists()

    def run(self) -> None:
        super().run()
        if not self.output().exists():
            ht = empty_variant_annotations_table(
                self.dataset_type,
                self.reference_genome,
            )
        ht = self.update(ht)
        write_ht(self.env, ht, self.output().path)

    def update(self, mt: hl.Table) -> hl.Table:
        return mt
