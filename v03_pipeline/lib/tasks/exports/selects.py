import hail as hl

from v03_pipeline.lib.annotations.expression_helpers import get_expr_for_xpos
from v03_pipeline.lib.model import DatasetType, SampleType


def get_entries_export_fields(
    ht: hl.Table,
    dataset_type: DatasetType,
    sample_type: SampleType,
    project_guid: str,
):
    return {
        'key_': ht.key_,
        'project_guid': project_guid,
        'family_guid': ht.family_entries.family_guid[0],
        'sample_type': sample_type.value,
        'xpos': get_expr_for_xpos(ht.locus),
        **(
            {
                'is_gnomad_gt_5_percent': hl.is_defined(ht.is_gt_5_percent),
            }
            if hasattr(ht, 'is_gt_5_percent')
            else {}
        ),
        'filters': ht.filters,
        'calls': ht.family_entries.map(
            lambda fe: dataset_type.entries_export_fields(fe),
        ),
        'sign': 1,
    }
