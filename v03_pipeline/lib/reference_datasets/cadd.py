import tempfile
import urllib

import hail as hl

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_datasets.utils import key_by_locus_alleles


# adapted from download_and_create_reference_datasets/v02/hail_scripts/write_cadd_ht.py
def import_cadd_table(
    raw_dataset_path: str,
    reference_genome: ReferenceGenome,
):
    column_names = {
        'f0': 'chrom',
        'f1': 'pos',
        'f2': 'ref',
        'f3': 'alt',
        'f4': 'RawScore',
        'f5': 'PHRED',
    }
    types = {'f0': hl.tstr, 'f1': hl.tint, 'f4': hl.tfloat32, 'f5': hl.tfloat32}

    with tempfile.NamedTemporaryFile(suffix='.tsv.gz', delete=False) as tmp_file:
        urllib.request.urlretrieve(raw_dataset_path, tmp_file.name)  # noqa: S310
        cadd_ht = hl.import_table(
            tmp_file,
            force_bgz=True,
            comment='#',
            no_header=True,
            types=types,
            min_partitions=10000,
        )
        cadd_ht = cadd_ht.rename(column_names)
        return key_by_locus_alleles(cadd_ht, reference_genome)


def get_ht(
    raw_dataset_paths: list[str],
    reference_genome: ReferenceGenome,
) -> hl.Table:
    snv_path, indel_path = raw_dataset_paths
    snvs_ht = import_cadd_table(snv_path, reference_genome)
    indel_ht = import_cadd_table(indel_path, reference_genome)
    ht = snvs_ht.union(indel_ht)
    return ht.select('PHRED')
