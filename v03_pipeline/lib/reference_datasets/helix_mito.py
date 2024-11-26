import shutil
import tempfile

import hail as hl
import requests

from v03_pipeline.lib.model.definitions import ReferenceGenome
from v03_pipeline.lib.reference_datasets.misc import safely_add_to_hdfs

RENAME = {
    'counts_hom': 'AC_hom',
    'counts_het': 'AC_het',
    'max_ARF': 'max_hl',
}


def get_ht(
    url: str,
    reference_genome: ReferenceGenome,
) -> hl.Table:
    with tempfile.NamedTemporaryFile(
        suffix='.tsv',
        delete=False,
    ) as tmp_file, requests.get(url, stream=True, timeout=10) as r:
        shutil.copyfileobj(r.raw, tmp_file)
        safely_add_to_hdfs(tmp_file.name)
    ht = hl.import_table(
        tmp_file.name,
        types={
            'counts_hom': hl.tint32,
            'counts_het': hl.tint32,
            'max_ARF': hl.tfloat32,
            'AF_het': hl.tfloat32,
            'AF_hom': hl.tfloat32,
            'alleles': hl.tarray(hl.tstr),
        },
    )
    ht = ht.rename(RENAME)
    ht = ht.select(
        *RENAME.values(),
        locus=hl.locus(
            'chrM',
            hl.parse_int32(ht.locus.split(':')[1]),
            reference_genome,
        ),
        alleles=ht.alleles,
        AN=hl.if_else(
            ht.AF_hom > 0,
            hl.int32(ht.AC_hom / ht.AF_hom),
            hl.int32(ht.AC_het / ht.AF_het),
        ),
        AF_hom=ht.AF_hom,
        AF_het=ht.AF_het,
    )
    return ht.key_by('locus', 'alleles')
