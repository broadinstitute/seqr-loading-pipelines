import luigi

from v03_pipeline.lib.core.definitions import ReferenceGenome
from v03_pipeline.lib.paths import reference_dataset_parquet
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, GCSorLocalTarget


class UpdatedReferenceDatasetParquetTask(luigi.Task):
    reference_genome = luigi.EnumParameter(enum=ReferenceGenome)
    reference_dataset: ReferenceDataset = luigi.EnumParameter(
        enum=ReferenceDataset,
    )

    def complete(self) -> luigi.Target:
        return GCSorLocalFolderTarget(self.output().path).exists()

    def output(self):
        return GCSorLocalTarget(
            reference_dataset_parquet(
                self.reference_genome,
                self.reference_dataset,
            ),
        )

    def create_table(self):
        df = self.reference_dataset.get_spark_dataframe(self.reference_genome)
        df.write.parquet(
            self.output().path,
            mode='overwrite',
        )
