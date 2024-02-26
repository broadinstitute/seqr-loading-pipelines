import hail as hl

from v03_pipeline.lib.model import DatasetType, ReferenceGenome


def validate_vep_config_reference_genome(reference_genome, config: str) -> None:
    with open(config) as f:
        if reference_genome.value not in f.read():
            msg = f'Vep config does not match supplied reference genome {reference_genome.value}'
            raise ValueError(msg)


def run_vep(
    ht: hl.Table,
    dataset_type: DatasetType,
    reference_genome: ReferenceGenome,
    vep_config_json_path: str | None,
) -> hl.Table:
    if not dataset_type.veppable:
        return ht
    config = (
        vep_config_json_path
        if vep_config_json_path is not None
        else f'file:///vep_data/vep-{reference_genome.value}-gcloud.json'
    )
    validate_vep_config_reference_genome(reference_genome, config)
    return hl.vep(
        ht,
        config=config,
        name='vep',
        block_size=1000,
        tolerate_parse_error=True,
    )
