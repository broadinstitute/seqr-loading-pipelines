import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.misc.validation import (
    get_validation_dependencies,
    validate_allele_type,
    validate_expected_contig_frequency,
    validate_imputed_sex_ploidy,
    validate_no_duplicate_variants,
    validate_sample_type,
)
from v03_pipeline.lib.model import CachedReferenceDatasetQuery
from v03_pipeline.lib.model.environment import Env
from v03_pipeline.lib.paths import (
    imported_callset_path,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_update import BaseUpdateTask
from v03_pipeline.lib.tasks.files import CallsetTask, GCSorLocalTarget
from v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query import (
    UpdatedCachedReferenceDatasetQuery,
)
from v03_pipeline.lib.tasks.write_imported_callset import WriteImportedCallsetTask
from v03_pipeline.lib.tasks.write_sex_check_table import WriteSexCheckTableTask


@luigi.util.inherits(BaseLoadingRunParams)
class ValidateCallsetTask(BaseUpdateTask):
    def complete(self) -> luigi.Target:
        if super().complete():
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
        if self.dataset_type.can_run_validation:
            # Rather than throwing an error, we silently remove invalid contigs.
            # This happens fairly often for AnVIL requests.
            mt = mt.filter_rows(
                hl.set(self.reference_genome.standard_contigs).contains(
                    mt.locus.contig,
                ),
            )

        if not self.skip_validation and self.dataset_type.can_run_validation:
            validation_dependencies = get_validation_dependencies(
                **self.param_kwargs,
            )
            validate_allele_type(
                mt,
                **self.param_kwargs,
                **validation_dependencies,
            )
            validate_no_duplicate_variants(
                mt,
                **self.param_kwargs,
                **validation_dependencies,
            )
            validate_expected_contig_frequency(
                mt,
                **self.param_kwargs,
                **validation_dependencies,
            )
            validate_sample_type(
                mt,
                **self.param_kwargs,
                **validation_dependencies,
            )
            validate_imputed_sex_ploidy(
                mt,
                **self.param_kwargs,
                **validation_dependencies,
            )
        return mt.select_globals(
            callset_path=self.callset_path,
            validated_sample_type=self.sample_type.value,
        )
