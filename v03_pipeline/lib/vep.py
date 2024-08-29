from string import Template

import hail as hl

from v03_pipeline.lib.model import DatasetType, ReferenceGenome

VEP_CONFIG_URI = Template('file:///vep_data/vep-$reference_genome.json')


def run_vep(
    ht: hl.Table,
    dataset_type: DatasetType,
    reference_genome: ReferenceGenome,
) -> hl.Table:
    if not dataset_type.veppable:
        return ht
    return hl.vep(
        ht,
        config=VEP_CONFIG_URI.substitute(reference_genome=reference_genome.value),
        name='vep',
        block_size=1000,
        tolerate_parse_error=True,
        csq=False,
    )
