import json

import hail as hl
import hailtop.fs as hfs
import luigi
import luigi.util

from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.model import SampleType
from v03_pipeline.lib.paths import (
    metadata_for_run_path,
    new_variants_table_path,
    pipeline_run_success_file_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    BaseReferenceDataset,
    ReferenceDataset,
)
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)
from v03_pipeline.lib.tasks.clickhouse_migration.constants import (
    ClickHouseMigrationType,
)
from v03_pipeline.lib.tasks.exports.write_new_clinvar_variants_parquet import (
    WriteNewClinvarVariantsParquetTask,
)
from v03_pipeline.lib.tasks.exports.write_new_transcripts_parquet import (
    WriteNewTranscriptsParquetTask,
)
from v03_pipeline.lib.tasks.exports.write_new_variants_parquet import (
    WriteNewVariantsParquetTask,
)
from v03_pipeline.lib.tasks.files import GCSorLocalTarget, HailTableTask

MAX_SNV_INDEL_ALLELE_LENGTH = 500


@luigi.util.inherits(BaseLoadingPipelineParams)
class MigrateVariantsToClickHouseTask(luigi.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dynamic_parquet_tasks = set()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            pipeline_run_success_file_path(
                self.reference_genome,
                self.dataset_type,
                ClickHouseMigrationType.VARIANTS.run_id,
            ),
        )

    def requires(self) -> list[luigi.Task]:
        return HailTableTask(
            variant_annotations_table_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )

    def complete(self):
        return len(self.dynamic_parquet_tasks) >= 1 and all(
            dynamic_parquet_tasks.complete()
            for dynamic_parquet_tasks in self.dynamic_parquet_tasks
        )

    def run(self):
        # First, move the existing annotations table to
        # the new_variants location.
        ht = hl.read_table(
            self.input().path,
        )
        if not hasattr(ht, 'key_'):
            ht = ht.add_index(name='key_')

        if self.dataset_type.filter_invalid_sites:
            ht = ht.filter(hl.len(ht.alleles[1]) < MAX_SNV_INDEL_ALLELE_LENGTH)

        write(
            ht,
            new_variants_table_path(
                self.reference_genome,
                self.dataset_type,
                ClickHouseMigrationType.VARIANTS.run_id,
            ),
        )

        # Then, write all dependent parquet tasks.
        self.dynamic_parquet_tasks.update(
            [
                self.clone(
                    WriteNewTranscriptsParquetTask,
                    # SampleType and Callset Path being required
                    # here is byproduct of the "place all variants"
                    # in the variants path" hack.  In theory
                    # it is possible to re-factor the parameters
                    # such that this isn't required, but it's left
                    # as out of scope for now.  Alternatively,
                    # we could inherit the functionality of these
                    # tasks without calling them directly, but
                    # that was also more code.
                    run_id=ClickHouseMigrationType.VARIANTS.run_id,
                    sample_type=SampleType.WGS,
                    callset_path=None,
                ),
                self.clone(
                    WriteNewVariantsParquetTask,
                    run_id=ClickHouseMigrationType.VARIANTS.run_id,
                    sample_type=SampleType.WGS,
                    callset_path=None,
                ),
                *(
                    [
                        self.clone(
                            WriteNewClinvarVariantsParquetTask,
                            run_id=ClickHouseMigrationType.VARIANTS.run_id,
                            sample_type=SampleType.WGS,
                            callset_path=None,
                        ),
                    ]
                    if (
                        ReferenceDataset.clinvar
                        in BaseReferenceDataset.for_reference_genome_dataset_type(
                            self.reference_genome,
                            self.dataset_type,
                        )
                    )
                    else []
                ),
            ],
        )
        yield self.dynamic_parquet_tasks

        metadata_json = {
            'migration_type': ClickHouseMigrationType.VARIANTS.value,
            'callsets': [],
            'run_id': ClickHouseMigrationType.VARIANTS.run_id,
            'sample_type': '',
            'project_guids': [],
            'family_samples': {},
            'failed_family_samples': {
                'missing_samples': {},
                'relatedness_check': {},
                'sex_check': {},
                'ploidy_check': {},
            },
            'relatedness_check_file_path': '',
            'sample_qc': {},
        }
        path = metadata_for_run_path(
            self.reference_genome,
            self.dataset_type,
            ClickHouseMigrationType.VARIANTS.run_id,
        )
        with hfs.open(path, mode='w') as f:
            json.dump(metadata_json, f)

        with self.output().open('w') as f:
            f.write('')
