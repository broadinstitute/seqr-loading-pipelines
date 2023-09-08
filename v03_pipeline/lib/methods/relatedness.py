from __future__ import annotations

import hail as hl

from v03_pipeline.lib.model import ReferenceGenome


# Stub port from https://github.com/broadinstitute/gnomad_qc/blob/9763d8a581665ed0be8bf2ea09d74e3cabc4806e/gnomad_qc/v2/resources/sample_qc.py#L40
def qc_mt_path(reference_genome: ReferenceGenome) -> str:
    ref_str = '.grch38' if reference_genome == ReferenceGenome.GRCh38 else ''
    return f'gs://gnomad/sample_qc/mt/gnomad.joint.high_callrate_common_biallelic_snps.pruned{ref_str}.mt'


# Stub port from https://github.com/broadinstitute/gnomad_qc/blame/9763d8a581665ed0be8bf2ea09d74e3cabc4806e/gnomad_qc/v3/resources/sample_qc.py#L219
def qc_path() -> str:
    return 'gs://gnomad/sample_qc/mt/genomes_v3.1/gnomad_v3.1_qc_mt_v2_sites_dense.mt'


def ld_prune(
    callset_mt: hl.MatrixTable,
    reference_genome: ReferenceGenome,
    use_gnomad_in_ld_prune: bool = True,
) -> hl.MatrixTable:
    # Borrow from gnomAD ld pruning
    if not use_gnomad_in_ld_prune:
        pruned_mt = hl.ld_prune(callset_mt.GT, r2=0.1)
    elif reference_genome == ReferenceGenome.GRCh37:
        pruned_mt = hl.read_matrix_table(qc_mt_path(reference_genome))
    elif reference_genome == ReferenceGenome.GRCh38:
        pruned_mt = hl.read_matrix_table(qc_path())
    return callset_mt.filter_rows(
        hl.is_defined(pruned_mt.index_rows(callset_mt.row_key)),
    )
