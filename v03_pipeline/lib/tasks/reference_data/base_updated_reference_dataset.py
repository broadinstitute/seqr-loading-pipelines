import hail as hl
import luigi

from luigi_pipeline.lib.hail_tasks import GCSorLocalTarget
from v03_pipeline.lib.paths import valid_reference_dataset_path
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask


@luigi.util.inherits(BaseLoadingRunParams)
class UpdatedReferenceDataset(BaseWriteTask):
    reference_dataset: ReferenceDataset

    def output(self):
        return GCSorLocalTarget(
            valid_reference_dataset_path(
                self.reference_genome,
                self.reference_dataset,
            ),
        )

    def create_table(self):
        ht = self.reference_dataset.get_ht(self.reference_genome)
        enum_select_fields = self.get_enum_select_fields(ht)
        if enum_select_fields:
            ht = ht.transmute(**enum_select_fields)
        return ht.annotate_globals(
            version=self.reference_dataset.version,
            enums=self.reference_dataset.enum_select or hl.missing(hl.tstruct(hl.tstr, hl.tarray(hl.tstr))),
        )

    def get_enum_select_fields(self, ht: hl.Table) -> dict:
        enum_selects = self.reference_dataset.enum_select
        enum_select_fields = {}
        if enum_selects is None:
            return enum_select_fields
        for field_name, values in enum_selects.items():
            lookup = hl.dict(
                hl.enumerate(values, index_first=False).extend(
                    # NB: adding missing values here allows us to
                    # hard fail if a mapped key is present and has an unexpected value
                    # but propagate missing values.
                    [(hl.missing(hl.tstr), hl.missing(hl.tint32))],
                ),
            )
            # NB: this conditioning on type is "outside" the hail expression context.
            if (
                    isinstance(ht[field_name].dtype, hl.tarray | hl.tset)
                    and ht[field_name].dtype.element_type == hl.tstr
            ):
                enum_select_fields[f'{field_name}_ids'] = ht[field_name].map(
                    lambda x: lookup[x],  # noqa: B023
                )
            else:
                enum_select_fields[f'{field_name}_id'] = lookup[ht[field_name]]
        return enum_select_fields
