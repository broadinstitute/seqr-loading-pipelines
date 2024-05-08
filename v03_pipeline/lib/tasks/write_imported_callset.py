import hail as hl
import luigi

from v03_pipeline.lib.misc.io import (
    import_callset,
    select_relevant_fields,
    split_multi_hts,
)
from v03_pipeline.lib.misc.validation import (
    validate_expected_contig_frequency,
    validate_imputed_sex_ploidy,
    validate_no_duplicate_variants,
    validate_sample_type,
)
from v03_pipeline.lib.misc.vets import annotate_vets
from v03_pipeline.lib.model import CachedReferenceDatasetQuery
from v03_pipeline.lib.paths import (
    imported_callset_path,
    sex_check_table_path,
    valid_cached_reference_dataset_query_path,
)
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask
from v03_pipeline.lib.tasks.files import CallsetTask, GCSorLocalTarget, HailTableTask
from v03_pipeline.lib.tasks.write_sex_check_table import WriteSexCheckTableTask


class WriteImportedCallsetTask(BaseWriteTask):
    callset_path = luigi.Parameter()
    imputed_sex_path = luigi.Parameter(default=None)
    filters_path = luigi.OptionalParameter(
        default=None,
        description='Optional path to part two outputs from callset (VCF shards containing filter information)',
    )
    validate = luigi.BoolParameter(
        default=True,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    force = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    check_sex_and_relatedness = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )

    def complete(self) -> luigi.Target:
        if not self.force and super().complete():
            mt = hl.read_matrix_table(self.output().path)
            return hasattr(mt, 'sample_type') and hl.eval(
                self.sample_type.value == mt.sample_type,
            )
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
        if (
            self.check_sex_and_relatedness
            and self.dataset_type.check_sex_and_relatedness
        ):
            requirements = [
                *requirements,
                WriteSexCheckTableTask(
                    self.reference_genome,
                    self.dataset_type,
                    self.sample_type,
                    self.callset_path,
                    self.imputed_sex_path,
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
        # Special handling of variant-level filter annotation for VETs filters.
        # The annotations are present on the sample-level FT field but are
        # expected upstream on "filters".
        mt = annotate_vets(mt)
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
        if (
            self.check_sex_and_relatedness
            and self.dataset_type.check_sex_and_relatedness
        ):
            sex_check_ht = hl.read_table(
                sex_check_table_path(
                    self.reference_genome,
                    self.dataset_type,
                    self.callset_path,
                ),
            )
            validate_imputed_sex_ploidy(
                mt,
                sex_check_ht,
            )
        return mt.annotate_globals(
            callset_path=self.callset_path,
            filters_path=self.filters_path or hl.missing(hl.tstr),
            sample_type=self.sample_type.value,
        )
