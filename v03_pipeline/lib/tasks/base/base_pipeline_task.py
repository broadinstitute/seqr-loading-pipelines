import hail as hl
import luigi

from v03_pipeline.lib.misc.io import write_ht
from v03_pipeline.lib.definitions import DatasetType, Env, ReferenceGenome


def empty_table(
    dataset_type: DatasetType,
    reference_genome: ReferenceGenome,
) -> hl.Table:
    key_type = dataset_type.table_key_type(reference_genome)
    return hl.Table.parallelize(
        [],
        key_type,
        key=key_type.fields,
    )


class BasePipelineTask(luigi.Task):
    env = luigi.EnumParameter(enum=Env, default=Env.LOCAL)
    reference_genome = luigi.EnumParameter(
        enum=ReferenceGenome,
        default=ReferenceGenome.GRCh38,
    )
    dataset_type = luigi.EnumParameter(enum=DatasetType)
    hail_temp_dir = luigi.OptionalParameter(
        default=None,
        description='Networked temporary directory used by hail for temporary file storage. Must be a network-visible file path.',
    )

    def run(self) -> None:
        if self.hail_temp_dir:
            # Need to use the GCP bucket as temp storage for very large callset joins
            hl.init(tmp_dir=self.hail_temp_dir)

        # Interval ref data join causes shuffle death, this prevents it
        hl._set_flags(use_new_shuffle='1')  # noqa: SLF001

        if not self.output().exists():
            ht = empty_table(
                self.dataset_type,
                self.reference_genome,
            )

        ht = self.update(ht)
        write_ht(self.env, ht, self.output().path)
