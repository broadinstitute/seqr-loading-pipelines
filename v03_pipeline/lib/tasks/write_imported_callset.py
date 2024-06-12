import hail as hl
import luigi

from v03_pipeline.lib.misc.io import (
    import_callset,
    select_relevant_fields,
    split_multi_hts,
)
from v03_pipeline.lib.misc.validation import (
    validate_allele_type,
    validate_expected_contig_frequency,
    validate_imported_field_types,
    validate_imputed_sex_ploidy,
    validate_no_duplicate_variants,
    validate_sample_type,
)
from v03_pipeline.lib.misc.vets import annotate_vets
from v03_pipeline.lib.model import CachedReferenceDatasetQuery
from v03_pipeline.lib.model.environment import Env
from v03_pipeline.lib.paths import (
    cached_reference_dataset_query_path,
    imported_callset_path,
    sex_check_table_path,
)
from v03_pipeline.lib.tasks.base.base_loading_params import BaseLoadingParams
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask
from v03_pipeline.lib.tasks.files import CallsetTask, GCSorLocalTarget, HailTableTask
from v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query import (
    UpdatedCachedReferenceDatasetQuery,
)
from v03_pipeline.lib.tasks.write_sex_check_table import WriteSexCheckTableTask

luigi.util.inherits(BaseLoadingParams)


class WriteImportedCallsetTask(BaseWriteTask):
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
            self.check_sex_and_relatedness
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

    def additional_row_fields(self, mt):
        return {
            **(
                {'info.AF': hl.tarray(hl.tfloat64)}
                if self.check_sex_and_relatedness
                and self.dataset_type.check_sex_and_relatedness
                else {}
            ),
            # this field is never required, the pipeline
            # will run smoothly even in its absence, but
            # will trigger special handling if it is present.
            **(
                {'info.CALIBRATION_SENSITIVITY': hl.tarray(hl.tstr)}
                if hasattr(mt, 'info') and hasattr(mt.info, 'CALIBRATION_SENSITIVITY')
                else {}
            ),
        }

    def create_table(self) -> hl.MatrixTable:
        mt = import_callset(
            self.callset_path,
            self.reference_genome,
            self.dataset_type,
            self.filters_path,
        )
        mt = select_relevant_fields(
            mt,
            self.dataset_type,
            self.additional_row_fields(mt),
        )
        # This validation isn't override-able.  If a field is the wrong
        # type, the pipeline will likely hard-fail downstream.
        validate_imported_field_types(
            mt,
            self.dataset_type,
            self.additional_row_fields(mt),
        )
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
