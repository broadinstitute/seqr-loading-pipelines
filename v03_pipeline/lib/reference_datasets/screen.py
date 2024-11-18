import shutil
import tempfile

import hail as hl
import requests

from v03_pipeline.lib.model import ReferenceGenome


# Adapted from download_and_create_reference_datasets/v02/hail_scripts/write_ccREs_ht.py
def make_interval_bed_table(ht: hl.Table, reference_genome: ReferenceGenome):
    """
    Remove the extra fields from the input ccREs file and mimic a bed import.
    """
    ht = ht.select(
        interval=hl.locus_interval(
            ht['f0'],
            ht['f1'] + 1,
            ht['f2'] + 1,
            reference_genome=reference_genome.value,
            invalid_missing=True,
        ),
        target=ht['f5'],
    )
    ht = ht.transmute(region_type=ht.target.split(','))
    return ht.key_by('interval')


def get_ht(raw_dataset_path: str, reference_genome: ReferenceGenome) -> hl.Table:
    with tempfile.NamedTemporaryFile(
        suffix='.bed',
        delete=False,
    ) as tmp_file, requests.get(
        raw_dataset_path,
        stream=True,
        timeout=10,
    ) as r:
        shutil.copyfileobj(r.raw, tmp_file)
        ht = hl.import_table(
            tmp_file.name,
            no_header=True,
            types={
                'f1': hl.tint32,
                'f2': hl.tint32,
            },
        )
        return make_interval_bed_table(ht, reference_genome)
