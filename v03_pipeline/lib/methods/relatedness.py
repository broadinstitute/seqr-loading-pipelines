from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl
from gnomad.sample_qc.pipeline import filter_rows_for_qc

from v03_pipeline.lib.model import CachedReferenceDatasetQuery
from v03_pipeline.lib.paths import valid_cached_reference_dataset_query_path

if TYPE_CHECKING:
    from v03_pipeline.lib.model import ReferenceGenome


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
    qnomad_qc_ht = hl.read_table(
        valid_cached_reference_dataset_query_path(
            reference_genome,
            CachedReferenceDatasetQuery.GNOMAD_QC,
        ),
    )
    return mt.filter_rows(
        hl.is_defined(qnomad_qc_ht[mt.row_key]),
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
    kin_ht = kin_ht.key_by('i', 'j')
    return kin_ht.select(
        ibd0=kin_ht.ibd.Z0,
        ibd1=kin_ht.ibd.Z1,
        ibd2=kin_ht.ibd.Z2,
        pi_hat=kin_ht.ibd.PI_HAT,
    )
