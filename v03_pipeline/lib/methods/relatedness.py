from __future__ import annotations

import hail as hl
from gnomad.sample_qc.pipeline import filter_rows_for_qc


def filter_and_ld_prune(
    mt: hl.MatrixTable,
    gnomad_qc_ht: hl.Table | None,
) -> hl.MatrixTable:
    mt = filter_rows_for_qc(
        mt,
        min_af=0.001,
        min_callrate=0.99,
        apply_hard_filters=False,
    )
    if not gnomad_qc_ht:
        mm_pruned = hl.ld_prune(mt.GT, r2=0.1)
        return mt.filter_rows(hl.is_defined(mm_pruned[mt.row_key]))
    return mt.filter_rows(
        hl.is_defined(gnomad_qc_ht[mt.row_key]),
    )


def call_relatedness(
    mt: hl.MatrixTable,  # NB: we've been remapped and subsetted upstream
    gnomad_qc_ht: hl.Table | None,
    af_field: str = 'info.AF',
) -> hl.Table:
    mt = filter_and_ld_prune(mt, gnomad_qc_ht)
    # NB: ibd did not work by default with my pip install of `hail` on an M1 MacOSX.
    # I had to build hail by source with the following:
    # - brew install lz4
    # - CXXFLAGS='-I/opt/homebrew/include/' HAIL_COMPILE_NATIVES=1 make -C hail install
    # Hail issue here: https://discuss.hail.is/t/noclassdeffounderror-could-not-initialize-class-is-hail-methods-ibsffi/2453
    kin_ht = hl.identity_by_descent(mt, maf=mt[af_field], min=0.10, max=1.0)
    kin_ht = kin_ht.key_by('i', 'j')
    return kin_ht.select(
        ibd0=kin_ht.ibd.Z0,
        ibd1=kin_ht.ibd.Z1,
        ibd2=kin_ht.ibd.Z2,
        pi_hat=kin_ht.ibd.PI_HAT,
    )


def build_relatedness_check_lookup(
    relatedness_check_ht: hl.Table,
    remap_lookup: hl.dict,
) -> dict[tuple[str, str], hl.Struct]:
    # Build relatedness check lookup
    relatedness_check_ht = relatedness_check_ht.key_by(
        i=remap_lookup.get(relatedness_check_ht.i, relatedness_check_ht.i),
        j=remap_lookup.get(relatedness_check_ht.j, relatedness_check_ht.j),
    )
    return {(r.i, r.j): r.drop('i', 'j') for r in relatedness_check_ht.collect()}
