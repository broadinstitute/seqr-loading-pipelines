import os
import shutil
import tempfile
import zipfile

import hail as hl
import requests

from v03_pipeline.lib.model.definitions import ReferenceGenome


def get_ht(
    url: str,
    reference_genome: ReferenceGenome,
) -> hl.Table:
    extracted_filename = url.removesuffix('.zip').split('/')[-1]
    with tempfile.NamedTemporaryFile(
        suffix='.txt.zip',
    ) as tmp_file, requests.get(url, stream=True, timeout=10) as r:
        shutil.copyfileobj(r.raw, tmp_file)
        with zipfile.ZipFile(tmp_file.name, 'r') as zipf:
            zipf.extractall(os.path.dirname(tmp_file.name))
        ht = hl.import_table(
            os.path.join(
                # Extracting the zip file
                os.path.dirname(tmp_file.name),
                extracted_filename,
            ),
        )
        ht = ht.select(
            locus=hl.locus('chrM', hl.parse_int32(ht.Start), reference_genome),
            alleles=[ht.Ref, ht.Alt],
            score=hl.parse_float32(ht.APOGEE2_score),
        )
        return ht.key_by('locus', 'alleles')
