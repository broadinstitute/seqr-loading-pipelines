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
        mm_pruned = hl.ld_prune(mt.GT, r2=0.1)
        return mt.filter_rows(hl.is_defined(mm_pruned[mt.row_key]))
    if reference_genome == ReferenceGenome.GRCh37:
        pruned_mt = hl.read_matrix_table(qc_mt_path(reference_genome))
    elif reference_genome == ReferenceGenome.GRCh38:
        pruned_mt = hl.read_matrix_table(qc_path())
    return mt.filter_rows(
        hl.is_defined(pruned_mt.index_rows(mt.row_key)),
    )


def annotate_families(
    ht: hl.Table,
    pedigree_ht: hl.Table,
) -> hl.Table:
    sample_id_to_family_guid = hl.dict(
        {x.s: x.family_guid for x in pedigree_ht.collect()},
    )
    return ht.annotate(
        fam_guid_i=sample_id_to_family_guid[ht.i],
        fam_guid_j=sample_id_to_family_guid[ht.j],
    )


def call_relatedness(
    mt: hl.MatrixTable,  # NB: we've been remapped and subsetted upstream
    reference_genome: ReferenceGenome,
    af_field: str = 'info.AF',
    use_gnomad_in_ld_prune: bool = True,
) -> hl.Table:
    mt = filter_and_ld_prune(mt, reference_genome, use_gnomad_in_ld_prune)
    # NB: ibd did not work by default with my pip install of `hail` on an M1 MacOSX.
    # I had to build hail by source with the following:
    # - brew install lz4
    # - CXXFLAGS='-I/opt/homebrew/include/' HAIL_COMPILE_NATIVES=1 make -C hail install
    # Hail issue here: https://discuss.hail.is/t/noclassdeffounderror-could-not-initialize-class-is-hail-methods-ibsffi/2453
    kin_ht = hl.identity_by_descent(mt, maf=mt[af_field], min=0.10, max=1.0)
    kin_ht = kin_ht.annotate(
        ibd0=kin_ht.ibd.Z0,
        ibd1=kin_ht.ibd.Z1,
        ibd2=kin_ht.ibd.Z2,
        pi_hat=kin_ht.ibd.PI_HAT,
    ).drop('ibs0', 'ibs1', 'ibs2', 'ibd')
    return kin_ht.key_by('i', 'j')
