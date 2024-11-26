import os

import hail as hl

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_datasets.misc import download_zip_file

EXTRACTED_FILE_NAME = 'supplementary_dataset_7.tsv'


def get_ht(url: str, reference_genome: ReferenceGenome) -> hl.Table:
    with download_zip_file(
        url,
        'local_constraint_mito',
        decode_content=True,
    ) as unzipped_dir:
        ht = hl.import_table(
            os.path.join(
                unzipped_dir,
                EXTRACTED_FILE_NAME,
            ),
        )
        ht = ht.select(
            locus=hl.locus('chrM', hl.parse_int32(ht.Position), reference_genome.value),
            alleles=[ht.Reference, ht.Alternate],
            score=hl.parse_float32(ht.MLC_score),
        )
        return ht.key_by('locus', 'alleles')
