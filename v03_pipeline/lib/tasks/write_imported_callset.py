import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.misc.callsets import get_additional_row_fields
from v03_pipeline.lib.misc.io import (
    import_callset,
    import_vcf,
    select_relevant_fields,
    split_multi_hts,
)
from v03_pipeline.lib.misc.validation import (
    validate_imported_field_types,
)
from v03_pipeline.lib.misc.vets import annotate_vets
from v03_pipeline.lib.model.feature_flag import FeatureFlag
from v03_pipeline.lib.paths import (
    imported_callset_path,
    valid_filters_path,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask
from v03_pipeline.lib.tasks.files import CallsetTask, GCSorLocalTarget
from v03_pipeline.lib.tasks.write_tdr_metrics_files import WriteTDRMetricsFilesTask
from v03_pipeline.lib.tasks.write_validation_errors_for_run import (
    with_persisted_validation_errors,
)


@luigi.util.inherits(BaseLoadingRunParams)
class WriteImportedCallsetTask(BaseWriteTask):
    def complete(self) -> luigi.Target:
        return super().complete()

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
        if (
            FeatureFlag.EXPECT_WES_FILTERS
            and not self.skip_expect_filters
            and self.dataset_type.expect_filters(
                self.sample_type,
            )
        ):
            requirements = [
                *requirements,
                CallsetTask(
                    valid_filters_path(
                        self.dataset_type,
                        self.sample_type,
                        self.callset_path,
                    ),
                ),
            ]
        if (
            FeatureFlag.EXPECT_TDR_METRICS
            and not self.skip_expect_tdr_metrics
            and self.dataset_type.expect_tdr_metrics(
                self.reference_genome,
            )
        ):
            requirements = [
                *requirements,
                self.clone(WriteTDRMetricsFilesTask),
            ]
        return [
            *requirements,
            CallsetTask(self.callset_path),
        ]

    @with_persisted_validation_errors
    def create_table(self) -> hl.MatrixTable:
        # NB: throws SeqrValidationError
        mt = import_callset(
            self.callset_path,
            self.reference_genome,
            self.dataset_type,
        )
        filters_path = None
        if (
            FeatureFlag.EXPECT_WES_FILTERS
            and not self.skip_expect_filters
            and self.dataset_type.expect_filters(
                self.sample_type,
            )
        ):
            filters_path = valid_filters_path(
                self.dataset_type,
                self.sample_type,
                self.callset_path,
            )
            filters_ht = import_vcf(filters_path, self.reference_genome).rows()
            mt = mt.annotate_rows(filters=filters_ht[mt.row_key].filters)
        additional_row_fields = get_additional_row_fields(
            mt,
            self.dataset_type,
            self.skip_check_sex_and_relatedness,
        )
        # NB: throws SeqrValidationError
        mt = select_relevant_fields(
            mt,
            self.dataset_type,
            additional_row_fields,
        )
        # This validation isn't override-able by the skip option.
        # If a field is the wrong type, the pipeline will likely hard-fail downstream.
        # NB: throws SeqrValidationError
        validate_imported_field_types(
            mt,
            self.dataset_type,
            additional_row_fields,
        )
        if self.dataset_type.has_multi_allelic_variants:
            # NB: throws SeqrValidationError
            mt = split_multi_hts(mt, self.skip_validation)

        # Special handling of variant-level filter annotation for VETs filters.
        # The annotations are present on the sample-level FT field but are
        # expected upstream on "filters".
        mt = annotate_vets(mt)
        return mt.select_globals(
            callset_path=self.callset_path,
            filters_path=filters_path or hl.missing(hl.tstr),
        )
