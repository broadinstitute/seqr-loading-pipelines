import hail as hl

from v03_pipeline.lib.model import ReferenceGenome


def get_ht(raw_dataset_path: str, reference_genome: ReferenceGenome) -> hl.Table:
    if reference_genome == ReferenceGenome.GRCh37:
        return hl.read_matrix_table(raw_dataset_path).rows()
    return hl.read_table(raw_dataset_path)
