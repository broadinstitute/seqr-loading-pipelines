import hail as hl

from v03_pipeline.lib.model import DatasetType


def run_vep(
    ht: hl.Table,
    dataset_type: DatasetType,
    vep_config_json_path: str | None,
) -> hl.Table:
    if not dataset_type.veppable:
        return ht
    config = (
        vep_config_json_path
        if vep_config_json_path is not None
        else 'file:///vep_data/vep-gcloud.json'
    )
    return hl.vep(
        ht,
        config=config,
        name='vep',
        block_size=1000,
        tolerate_parse_error=True,
    )
