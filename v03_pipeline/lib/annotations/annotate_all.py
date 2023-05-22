"""
These are copies of the identical functions present on SeqrVCFToMTTask in `seqr_loading.py`
and HailMatrixTableTask from `hail_tasks.py` from the v02 pipeline.
"""

import hail as hl

import luigi_pipeline.lib.hail_vep_runners as vep_runners
from v03_pipeline.lib.definitions import DatasetType, Env, ReferenceGenome


def run_vep(
    mt: hl.MatrixTable,
    env: Env,
    reference_genome: ReferenceGenome,
    vep_config_json_path: str,
):
    vep_runner = (
        vep_runners.HailVEPRunner()
        if env != Env.TEST
        else vep_runners.HailVEPDummyRunner()
    )
    return vep_runner.run(
        mt,
        reference_genome.v02_value,
        vep_config_json_path=vep_config_json_path,
    )


def add_37_coordinates(
    mt: hl.MatrixTable,
    liftover_ref_path: str,
):
    rg37 = hl.get_reference(ReferenceGenome.GRCh37.value)
    rg38 = hl.get_reference(ReferenceGenome.GRCh38.value)
    if not rg38.has_liftover(rg37):
        rg38.add_liftover(liftover_ref_path, rg37)
    return mt.annotate_rows(
        rg37_locus=hl.liftover(mt.locus, ReferenceGenome.GRCh37.value),
    )


def annotate_old_and_split_multi_hts(mt: hl.MatrixTable):
    """
    Saves the old allele and locus because while split_multi does this, split_multi_hts drops this. Will see if
    we can add this to split_multi_hts and then this will be deprecated.
    :return: mt that has pre-annotations
    """
    # Named `locus_old` instead of `old_locus` because split_multi_hts drops `old_locus`.
    return hl.split_multi_hts(
        mt.annotate_rows(locus_old=mt.locus, alleles_old=mt.alleles),
    )


def annotate_all(
    mt: hl.MatrixTable,
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    liftover_ref_path: str,
    vep_config_json_path: str,
):
    mt = annotate_old_and_split_multi_hts(mt)
    if reference_genome == ReferenceGenome.GRCh38 and dataset_type.should_add_liftover:
        mt = add_37_coordinates(mt, liftover_ref_path)
    if dataset_type.should_run_vep:
        mt = run_vep(mt, env, reference_genome, vep_config_json_path)

    # TODO, add the rest of the dataset_type specific annotations
    return mt.select_rows('vep', 'filters', 'rsid')
