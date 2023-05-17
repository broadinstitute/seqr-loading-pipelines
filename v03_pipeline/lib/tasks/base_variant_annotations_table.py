import hail as hl
import luigi

from v03_pipeline.lib.definitions import DatasetType, ReferenceGenome
from v03_pipeline.lib.paths import variant_annotations_table_path
from v03_pipeline.lib.tasks.base_pipeline_task import BasePipelineTask
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, GCSorLocalTarget


def empty_variant_annotations_mt(
    dataset_type: DatasetType,
    reference_genome: ReferenceGenome,
):
    ht = hl.Table.parallelize(
        [],
        key=dataset_type.variant_annotations_key(reference_genome),
    )
    # Danger! `from_rows_table` is experimental... but it was by far the
    # cleanest way to accomplish this.
    return hl.MatrixTable.from_rows_table(ht)


class BaseVariantAnnotationsTable(BasePipelineTask):
    @property
    def _variant_annotations_table_path(self) -> str:
        return variant_annotations_table_path(
            self.env,
            self.reference_genome,
            self.dataset_type,
        )

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(self._variant_annotations_table_path)

    def complete(self) -> bool:
        return GCSorLocalFolderTarget(self._variant_annotations_table_path).exists()

    def run(self) -> None:
        if not self.output().exists():
            mt = empty_variant_annotations_mt(self.dataset_type, self.reference_genome)
        self.update(mt)
        mt.write(self.output().path)

    def update(self, mt):
        pass
