import hail as hl

from v03_pipeline.lib.model.definitions import ReferenceGenome


def add_rg38_liftover(grch38_to_grch37_liftover_ref_path: str) -> None:
    rg37 = hl.get_reference(ReferenceGenome.GRCh37.value)
    rg38 = hl.get_reference(ReferenceGenome.GRCh38.value)
    if not rg38.has_liftover(rg37):
        rg38.add_liftover(grch38_to_grch37_liftover_ref_path, rg37)


def add_rg37_liftover(grch37_to_grch38_liftover_ref_path: str) -> None:
    rg37 = hl.get_reference(ReferenceGenome.GRCh37.value)
    rg38 = hl.get_reference(ReferenceGenome.GRCh38.value)
    if not rg37.has_liftover(rg38):
        rg37.add_liftover(grch37_to_grch38_liftover_ref_path, rg38)


def remove_liftover():
    rg37 = hl.get_reference(ReferenceGenome.GRCh37.value)
    rg38 = hl.get_reference(ReferenceGenome.GRCh38.value)
    if rg37.has_liftover(rg38):
        rg37.remove_liftover(rg38)
    if rg38.has_liftover(rg37):
        rg38.remove_liftover(rg37)
