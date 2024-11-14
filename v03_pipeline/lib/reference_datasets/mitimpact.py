import os

import hail as hl

from v03_pipeline.lib.model.definitions import ReferenceGenome
from v03_pipeline.lib.reference_datasets.misc import download_zip_file


def get_ht(
    url: str,
    reference_genome: ReferenceGenome,
) -> hl.Table:
    extracted_filename = url.removesuffix('.zip').split('/')[-1]
    with download_zip_file(url, suffix='.txt.zip') as unzipped_dir:
        ht = hl.import_table(
            os.path.join(
                unzipped_dir,
                extracted_filename,
            ),
        )
        ht = ht.select(
            locus=hl.locus('chrM', hl.parse_int32(ht.Start), reference_genome),
            alleles=[ht.Ref, ht.Alt],
            score=hl.parse_float32(ht.APOGEE2_score),
        )
        return ht.key_by('locus', 'alleles')
