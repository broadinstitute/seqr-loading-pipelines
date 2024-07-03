import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.misc.io import (
    import_callset,
    import_vcf,
    select_relevant_fields,
    split_multi_hts,
)
from v03_pipeline.lib.misc.vets import annotate_vets
from v03_pipeline.lib.model.environment import Env
from v03_pipeline.lib.paths import (
    imported_callset_path,
    valid_filters_path,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask
from v03_pipeline.lib.tasks.files import CallsetTask, GCSorLocalTarget


@luigi.util.inherits(BaseLoadingRunParams)
class WriteImportedCallsetTask(BaseWriteTask):
    def complete(self) -> luigi.Target:
        return not self.force and super().complete()

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
            Env.EXPECT_WES_FILTERS
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
        return [
            *requirements,
            CallsetTask(self.callset_path),
        ]

    def additional_row_fields(self, mt):
        return {
            **(
                {'info.AF': hl.tarray(hl.tfloat64)}
                if not self.skip_check_sex_and_relatedness
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
        )
        filters_path = None
        if (
            Env.EXPECT_WES_FILTERS
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
        mt = select_relevant_fields(
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
        return mt.select_globals(
            callset_path=self.callset_path,
            filters_path=filters_path or hl.missing(hl.tstr),
        )
