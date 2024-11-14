import hail as hl

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_datasets.misc import get_enum_select_fields

HGMD_CLASSES = [
    'DM',
    'DM?',
    'DP',
    'DFP',
    'FP',
    'R',
]

ENUMS = {'class': HGMD_CLASSES}


def get_ht(raw_dataset_path: str, reference_genome: ReferenceGenome) -> hl.Table:
    mt = hl.import_vcf(
        raw_dataset_path,
        reference_genome=reference_genome.value,
        force=True,
        min_partitions=100,
        skip_invalid_loci=True,
        contig_recoding=reference_genome.contig_recoding(),
    )
    ht = mt.rows()
    ht = ht.select(
        **{
            'accession': ht.rsid,
            'class': ht.info.CLASS,
        },
    )
    return ht.transmute(**get_enum_select_fields(ht, ENUMS))
