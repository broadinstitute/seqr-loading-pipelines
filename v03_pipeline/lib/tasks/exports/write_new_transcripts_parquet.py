import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.paths import (
    new_transcripts_parquet_path,
    new_variants_table_path,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.base.base_write_parquet import BaseWriteParquetTask
from v03_pipeline.lib.tasks.exports.misc import (
    camelcase_array_structexpression_fields,
    sorted_hl_struct,
    transcripts_field_name,
    unmap_formatting_annotation_enums,
)
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, GCSorLocalTarget
from v03_pipeline.lib.tasks.update_new_variants_with_caids import (
    UpdateNewVariantsWithCAIDsTask,
)
from v03_pipeline.lib.tasks.write_new_variants_table import WriteNewVariantsTableTask


@luigi.util.inherits(BaseLoadingRunParams)
class WriteNewTranscriptsParquetTask(BaseWriteParquetTask):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            new_transcripts_parquet_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

    def complete(self) -> luigi.Target:
        return GCSorLocalFolderTarget(self.output().path).exists()

    def requires(self) -> list[luigi.Task]:
        return [
            self.clone(UpdateNewVariantsWithCAIDsTask)
            if self.dataset_type.should_send_to_allele_registry
            else self.clone(WriteNewVariantsTableTask),
        ]

    def create_table(self) -> None:
        ht = hl.read_table(
            new_variants_table_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )
        ht = unmap_formatting_annotation_enums(
            ht,
            self.reference_genome,
            self.dataset_type,
        )
        ht = camelcase_array_structexpression_fields(ht, self.reference_genome)
        ht = ht.key_by()
        return ht.select(
            key_=ht.key_,
            transcripts=hl.enumerate(
                ht[transcripts_field_name(self.reference_genome, self.dataset_type)],
            )
            .starmap(
                lambda i, s: s.annotate(
                    majorConsequence=s.consequenceTerms.first(),
                    transcriptRank=i,
                ),
            )
            .map(sorted_hl_struct),
        )
