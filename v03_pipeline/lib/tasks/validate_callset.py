import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.misc.callsets import additional_row_fields
from v03_pipeline.lib.misc.validation import (
    validate_allele_type,
    validate_expected_contig_frequency,
    validate_imported_field_types,
    validate_imputed_sex_ploidy,
    validate_no_duplicate_variants,
    validate_sample_type,
)
from v03_pipeline.lib.model import CachedReferenceDatasetQuery
from v03_pipeline.lib.model.environment import Env
from v03_pipeline.lib.paths import (
    cached_reference_dataset_query_path,
    imported_callset_path,
    sex_check_table_path,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_update import BaseUpdateTask
from v03_pipeline.lib.tasks.files import CallsetTask, GCSorLocalTarget, HailTableTask
from v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query import (
    UpdatedCachedReferenceDatasetQuery,
)
from v03_pipeline.lib.tasks.write_imported_callset import WriteImportedCallsetTask
from v03_pipeline.lib.tasks.write_sex_check_table import WriteSexCheckTableTask


@luigi.util.inherits(BaseLoadingRunParams)
class ValidateCallsetTask(BaseUpdateTask):
    def complete(self) -> luigi.Target:
        if not self.force and super().complete():
            mt = hl.read_matrix_table(self.output().path)
            return hasattr(mt, 'validated_sample_type') and hl.eval(
                self.sample_type.value == mt.validated_sample_type,
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
        requirements = [
            self.clone(WriteImportedCallsetTask),
        ]
        if not self.skip_validation and self.dataset_type.can_run_validation:
            requirements = [
                *requirements,
                (
                    self.clone(
                        UpdatedCachedReferenceDatasetQuery,
                        crdq=CachedReferenceDatasetQuery.GNOMAD_CODING_AND_NONCODING_VARIANTS,
                    )
                    if Env.REFERENCE_DATA_AUTO_UPDATE
                    else HailTableTask(
                        cached_reference_dataset_query_path(
                            self.reference_genome,
                            self.dataset_type,
                            CachedReferenceDatasetQuery.GNOMAD_CODING_AND_NONCODING_VARIANTS,
                        ),
                    ),
                ),
            ]
        if (
            Env.CHECK_SEX_AND_RELATEDNESS
            and not self.skip_check_sex_and_relatedness
            and self.dataset_type.check_sex_and_relatedness
        ):
            requirements = [
                *requirements,
                self.clone(WriteSexCheckTableTask),
            ]
        return [
            *requirements,
            CallsetTask(self.callset_path),
        ]

    def update_table(self, mt: hl.MatrixTable) -> hl.MatrixTable:
        mt = hl.read_matrix_table(
            imported_callset_path(
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
            ),
        )
        # This validation isn't override-able.  If a field is the wrong
        # type, the pipeline will likely hard-fail downstream.
        validate_imported_field_types(
            mt,
            self.dataset_type,
            additional_row_fields(
                mt,
                self.dataset_type,
                self.skip_check_sex_and_relatedness,
            ),
        )
        if self.dataset_type.can_run_validation:
            # Rather than throwing an error, we silently remove invalid contigs.
            # This happens fairly often for AnVIL requests.
            mt = mt.filter_rows(
                hl.set(self.reference_genome.standard_contigs).contains(
                    mt.locus.contig,
                ),
            )

        if not self.skip_validation and self.dataset_type.can_run_validation:
            validate_allele_type(mt)
            validate_no_duplicate_variants(mt)
            validate_expected_contig_frequency(mt, self.reference_genome)
            coding_and_noncoding_ht = hl.read_table(
                cached_reference_dataset_query_path(
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
                Env.CHECK_SEX_AND_RELATEDNESS
                and not self.skip_check_sex_and_relatedness
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
        return mt.select_globals(
            callset_path=self.callset_path,
            validated_sample_type=self.sample_type.value,
        )
