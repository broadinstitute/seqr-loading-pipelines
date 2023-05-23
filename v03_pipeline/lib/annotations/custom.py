import hail as hl

import luigi_pipeline.lib.hail_vep_runners as vep_runners
from v03_pipeline.lib.definitions import DatasetType, Env, ReferenceGenome


def annotate_old_and_split_multi_hts(
    mt: hl.MatrixTable,
    dataset_type: DatasetType,
    **kwargs,
) -> hl.MatrixTable:
    if not (dataset_type == DatasetType.SNV or dataset_type == DatasetType.MITO):
        return mt
    return hl.split_multi_hts(
        mt.annotate_rows(locus_old=mt.locus, alleles_old=mt.alleles),
    )


def rg37_locus(
    mt: hl.MatrixTable,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    liftover_ref_path: str,
    **kwargs,
) -> hl.MatrixTable:
    if reference_genome == ReferenceGenome.GRCh37 or dataset_type == DatasetType.GCNV:
        return mt
    rg37 = hl.get_reference(ReferenceGenome.GRCh37.value)
    rg38 = hl.get_reference(ReferenceGenome.GRCh38.value)
    if not rg38.has_liftover(rg37):
        rg38.add_liftover(liftover_ref_path, rg37)
    return mt.annotate_rows(
        rg37_locus=hl.liftover(mt.locus, ReferenceGenome.GRCh37.value),
    )


def run_vep(
    mt: hl.MatrixTable,
    env: Env,
    dataset_type: DatasetType,
    reference_genome: ReferenceGenome,
    vep_config_json_path: str,
    **kwargs,
) -> hl.MatrixTable:
    if dataset_type != DatasetType.SNV:
        return mt
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
