import hail as hl
import luigi

from v03_pipeline.lib.misc.io import (
    import_callset,
    select_relevant_fields,
    split_multi_hts,
)
from v03_pipeline.lib.misc.validation import (
    validate_expected_contig_frequency,
    validate_no_duplicate_variants,
    validate_sample_type,
)
from v03_pipeline.lib.model import CachedReferenceDatasetQuery
from v03_pipeline.lib.paths import (
    imported_callset_path,
    valid_cached_reference_dataset_query_path,
)
from v03_pipeline.lib.tasks.base.base_write_task import BaseWriteTask
from v03_pipeline.lib.tasks.files import CallsetTask, GCSorLocalTarget, HailTableTask


class WriteImportedCallsetTask(BaseWriteTask):
    callset_path = luigi.Parameter()
    filters_path = luigi.OptionalParameter(
        default=None,
        description='Optional path to part two outputs from callset (VCF shards containing filter information)',
    )
    validate = luigi.BoolParameter(
        default=True,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )

    def complete(self) -> luigi.Target:
        if super().complete():
            mt = hl.read_matrix_table(self.output().path)
            return hasattr(mt, 'sample_type') and hl.eval(self.sample_type.value == mt.sample_type)
        return False

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            imported_callset_path(
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
            ),
        )

    def requires(self) -> list[luigi.Task]:
        requirements = []
        if self.filters_path:
            requirements = [
                *requirements,
                CallsetTask(self.filters_path),
            ]
        if self.validate and self.dataset_type.can_run_validation:
            requirements = [
                *requirements,
                HailTableTask(
                    valid_cached_reference_dataset_query_path(
                        self.reference_genome,
                        self.dataset_type,
                        CachedReferenceDatasetQuery.GNOMAD_CODING_AND_NONCODING_VARIANTS,
                    ),
                ),
            ]
        return [
            *requirements,
            CallsetTask(self.callset_path),
        ]

    def create_table(self) -> hl.MatrixTable:
        mt = import_callset(
            self.callset_path,
            self.reference_genome,
            self.dataset_type,
            self.filters_path,
        )
        mt = select_relevant_fields(mt, self.dataset_type)
        if self.dataset_type.has_multi_allelic_variants:
            mt = split_multi_hts(mt)
        if self.dataset_type.can_run_validation:
            # Rather than throwing an error, we silently remove invalid contigs.
            # This happens fairly often for AnVIL requests.
            mt = mt.filter_rows(
                hl.set(self.reference_genome.standard_contigs).contains(
                    mt.locus.contig,
                ),
            )
        if self.validate and self.dataset_type.can_run_validation:
            validate_no_duplicate_variants(mt)
            validate_expected_contig_frequency(mt, self.reference_genome)
            coding_and_noncoding_ht = hl.read_table(
                valid_cached_reference_dataset_query_path(
                    self.reference_genome,
                    self.dataset_type,
                    CachedReferenceDatasetQuery.GNOMAD_CODING_AND_NONCODING_VARIANTS,
                ),
            )
            validate_sample_type(
                mt,
                coding_and_noncoding_ht,
                self.reference_genome,
                self.sample_type,
            )
        return mt.annotate_globals(
            callset_path=self.callset_path,
            filters_path=hl.if_else(self.filters_path != None, self.filters_path, hl.missing(hl.tstr)),
            sample_type=self.sample_type.value,
        )
