import hail as hl
import luigi

from v03_pipeline.lib.definitions import DatasetType, ReferenceGenome
from v03_pipeline.lib.paths import new_checkpoint_path, variant_annotations_table_path
from v03_pipeline.lib.tasks.base.base_pipeline_task import BasePipelineTask
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, GCSorLocalTarget


def empty_variant_annotations_table(
    dataset_type: DatasetType,
    reference_genome: ReferenceGenome,
) -> hl.MatrixTable:
    key_type = dataset_type.variant_annotations_table_key_type(reference_genome)
    ht = hl.Table.parallelize(
        [],
        key_type,
        key=key_type.fields,
    )
    # Danger! `from_rows_table` is experimental... but it was by far the
    # cleanest way to accomplish this.
    return hl.MatrixTable.from_rows_table(ht)


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
            mt = empty_variant_annotations_table(
                self.dataset_type,
                self.reference_genome,
            )
        mt = self.update(mt)
        mt = mt.checkpoint(new_checkpoint_path(self.env), stage_locally=True, overwrite=True)
        mt.write(self.output().path, stage_locally=True, overwrite=True)

    def update(self, mt: hl.MatrixTable) -> hl.MatrixTable:
        return mt
