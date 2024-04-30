import hail as hl

from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome


def validate_vep_config_reference_genome(reference_genome) -> None:
    with open(Env.VEP_CONFIG_PATH) as f:
        if reference_genome.value not in f.read():
            msg = f'Vep config does not match supplied reference genome {reference_genome.value}'
            raise ValueError(msg)


def run_vep(
    ht: hl.Table,
    dataset_type: DatasetType,
    reference_genome: ReferenceGenome,
) -> hl.Table:
    if not dataset_type.veppable:
        return ht
    validate_vep_config_reference_genome(reference_genome)
    return hl.vep(
        ht,
        config=Env.VEP_CONFIG_URI,
        name='vep',
        block_size=1000,
        tolerate_parse_error=True,
        csq=False,
    )
