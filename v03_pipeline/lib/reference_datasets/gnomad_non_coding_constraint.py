import hail as hl

from v03_pipeline.lib.core import ReferenceGenome
from v03_pipeline.lib.reference_datasets.misc import (
    select_for_interval_reference_dataset,
)


def get_ht(path: str, reference_genome: ReferenceGenome) -> hl.Table:
    ht = hl.import_table(
        path,
        types={
            'start': hl.tint32,
            'end': hl.tint32,
            'z': hl.tfloat32,
        },
        force_bgz=True,
    )
    return select_for_interval_reference_dataset(
        ht,
        reference_genome,
        {'z_score': ht['z']},
    )
