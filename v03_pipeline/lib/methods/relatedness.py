from __future__ import annotations

import hail as hl
from gnomad.sample_qc.pipeline import filter_rows_for_qc

from v03_pipeline.lib.model import ReferenceGenome


# Stub port from https://github.com/broadinstitute/gnomad_qc/blob/9763d8a581665ed0be8bf2ea09d74e3cabc4806e/gnomad_qc/v2/resources/sample_qc.py#L40
def qc_mt_path(reference_genome: ReferenceGenome) -> str:
    ref_str = '.grch38' if reference_genome == ReferenceGenome.GRCh38 else ''
    return f'gs://gnomad/sample_qc/mt/gnomad.joint.high_callrate_common_biallelic_snps.pruned{ref_str}.mt'


# Stub port from https://github.com/broadinstitute/gnomad_qc/blame/9763d8a581665ed0be8bf2ea09d74e3cabc4806e/gnomad_qc/v3/resources/sample_qc.py#L219
def qc_path() -> str:
    return 'gs://gnomad/sample_qc/mt/genomes_v3.1/gnomad_v3.1_qc_mt_v2_sites_dense.mt'


def filter_and_ld_prune(
    mt: hl.MatrixTable,
    reference_genome: ReferenceGenome,
    use_gnomad_in_ld_prune: bool,
) -> hl.MatrixTable:
    mt = filter_rows_for_qc(
        mt,
        min_af=0.001,
        min_callrate=0.99,
        apply_hard_filters=False,
    )
    if not use_gnomad_in_ld_prune:
        pruned_mt = hl.ld_prune(mt.GT, r2=0.1)
    elif reference_genome == ReferenceGenome.GRCh37:
        pruned_mt = hl.read_matrix_table(qc_mt_path(reference_genome))
    elif reference_genome == ReferenceGenome.GRCh38:
        pruned_mt = hl.read_matrix_table(qc_path())
    return mt.filter_rows(
        hl.is_defined(pruned_mt.index_rows(mt.row_key)),
    )


def call_relatedness(
    mt: hl.MatrixTable,  # NB: we've been remapped and subsetted upstream
    reference_genome: ReferenceGenome,
    use_gnomad_in_ld_prune: bool = True,
) -> hl.Table:
    mt = filter_and_ld_prune(mt, reference_genome, use_gnomad_in_ld_prune)
    kin_ht = hl.identity_by_descent(mt, maf=mt.af, min=0.10, max=1.0)
    kin_ht = kin_ht.annotate(
        ibd0=kin_ht.ibd.Z0,
        ibd1=kin_ht.ibd.Z1,
        ibd2=kin_ht.ibd.Z2,
        pi_hat=kin_ht.ibd.PI_HAT,
    ).drop('ibs0', 'ibs1', 'ibs2', 'ibd')
    kin_ht = kin_ht.key_by('i', 'j')
    return kin_ht
