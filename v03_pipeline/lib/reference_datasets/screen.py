import shutil
import tempfile

import hail as hl
import requests

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_datasets.misc import (
    select_for_interval_reference_dataset,
)


# Adapted from download_and_create_reference_datasets/v02/hail_scripts/write_ccREs_ht.py
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
    return select_for_interval_reference_dataset(
        ht,
        reference_genome,
        {'region_type': ht['f5'].split(',')},
        chrom_field='f0',
        start_field='f1',
        end_field='f2',
    )