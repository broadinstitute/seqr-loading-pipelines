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


def rg37_locus(
    mt: hl.MatrixTable,
    reference_genome: ReferenceGenome,
    liftover_ref_path: str,
):
    if reference_genome == ReferenceGenome.GRCh37:
        return mt
    rg37 = hl.get_reference(ReferenceGenome.GRCh37.value)
    rg38 = hl.get_reference(ReferenceGenome.GRCh38.value)
    if not rg38.has_liftover(rg37):
        rg38.add_liftover(liftover_ref_path, rg37)
    return mt.annotate_rows(
        rg37_locus=hl.liftover(mt.locus, ReferenceGenome.GRCh37.value),
    )


def annotate_all(
    mt: hl.MatrixTable,
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    liftover_ref_path: str,
    vep_config_json_path: str,
):
    # Special cases that require hail function calls.
    if dataset_type != DatasetType.GCNV:
        mt = rg37_locus(mt, reference_genome, liftover_ref_path)
    if dataset_type == DatasetType.SNV or dataset_type == DatasetType.MITO:
        mt = run_vep(mt, env, reference_genome, vep_config_json_path)

    # TODO, add the rest of the dataset_type specific annotations
    return mt.select_rows('vep', 'filters', 'rsid')
