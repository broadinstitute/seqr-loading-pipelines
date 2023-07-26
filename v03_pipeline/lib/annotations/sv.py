from __future__ import annotations

from typing import Any

import hail as hl

from v03_pipeline.lib.annotations.enums import SV_TYPE_DETAILS, SV_TYPES
from v03_pipeline.lib.model.definitions import ReferenceGenome

# TODO: remove me once hail is used.

BOTHSIDES_SUPPORT = 'BOTHSIDES_SUPPORT'
PASS = 'PASS'  # noqa: S105

SV_TYPES_LOOKUP = hl.dict(hl.enumerate(SV_TYPES, index_first=False))
SV_TYPE_DETAILS_LOOKUP = hl.dict(hl.enumerate(SV_TYPE_DETAILS, index_first=False))


def _get_cpx_interval(x):
    # an example format of CPX_INTERVALS is "DUP_chr1:1499897-1499974"
    type_chr = x.split('_chr')
    chr_pos = type_chr[1].split(':')
    pos = chr_pos[1].split('-')
    return hl.struct(
        type=type_chr[0],
        chrom=chr_pos[0],
        start=hl.int32(pos[0]),
        end=hl.int32(pos[1]),
    )


def _sv_types(ht: hl.Table) -> hl.ArrayExpression:
    return ht.alleles[1].replace('[<>]', '').split(':', 2)


def algorithms(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.str(',').join(ht.info.ALGORITHMS)


def bothsides_support(ht: hl.Table, **_: Any) -> hl.Expression:
    return ht.filters.any(lambda x: x == BOTHSIDES_SUPPORT)


def cpx_intervals(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.or_missing(
        hl.is_defined(ht.info.CPX_INTERVALS),
        ht.info.CPX_INTERVALS.map(lambda x: _get_cpx_interval(x)),
    )


def filters(ht: hl.Table, **_: Any) -> hl.Expression:
    filters = ht.filters.filter(lambda x: (x != PASS) & (x != BOTHSIDES_SUPPORT))
    return hl.or_missing(filters.size() > 0, filters)


def gnomad_svs(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.or_missing(
        hl.is_defined(ht.info.gnomAD_V2_AF),
        hl.struct(AF=ht.info.gnomAD_V2_AF, ID=ht.info.gnomAD_V2_SVID),
    )


def gt_stats(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.struct(
        AF=ht.info.AF[0],
        AC=ht.info.AC[0],
        AN=ht.info.AN,
        Hom=ht.info.N_HOMALT,
        Het=ht.info.N_HET,
    )


def rg37_locus_end(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    liftover_ref_path: str,
    **_: Any,
) -> hl.Expression | None:
    if reference_genome == ReferenceGenome.GRCh37:
        return None
    rg37 = hl.get_reference(ReferenceGenome.GRCh37.value)
    rg38 = hl.get_reference(ReferenceGenome.GRCh38.value)
    if not rg38.has_liftover(rg37):
        rg38.add_liftover(liftover_ref_path, rg37)
    end_locus = hl.if_else(
        hl.is_defined(ht.info.END2),
        hl.struct(contig=ht.info.CHR2, position=ht.info.END2),
        hl.struct(contig=ht.locus.contig, position=ht.info.END),
    )
    return hl.or_missing(
        end_locus.position
        <= hl.literal(hl.get_reference(ReferenceGenome.GRCh38.value).lengths)[
            end_locus.contig
        ],
        hl.liftover(
            hl.locus(
                end_locus.contig,
                end_locus.position,
                reference_genome=ReferenceGenome.GRCh38.value,
            ),
            ReferenceGenome.GRCh37.value,
        ),
    )


def strvctvre(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.struct(score=hl.parse_float(ht.info.StrVCTVRE))


def sv_type_id(ht: hl.Table, **_: Any) -> hl.Expression:
    return SV_TYPES_LOOKUP[_sv_types(ht)[0]]


def sv_type_detail_id(ht: hl.Table, **_: Any) -> hl.Expression:
    sv_types = _sv_types(ht)
    return hl.if_else(
        sv_types[0] == 'CPX',
        SV_TYPE_DETAILS_LOOKUP[ht.info.CPX_TYPE],
        hl.or_missing(
            (sv_types[0] == 'INS') & (hl.len(sv_types) > 1),
            SV_TYPE_DETAILS_LOOKUP[sv_types[1]],
        ),
    )
