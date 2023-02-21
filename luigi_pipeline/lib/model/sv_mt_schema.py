import hail as hl

from lib.model.base_mt_schema import BaseMTSchema, row_annotation, RowAnnotationOmit
from lib.model.seqr_mt_schema import SeqrVariantsAndGenotypesSchema

from hail_scripts.computed_fields import variant_id


BOTHSIDES_SUPPORT = "BOTHSIDES_SUPPORT"
PASS = "PASS"


INTERVAL_TYPE = 'array<struct{type: str, chrom: str, start: int32, end: int32}>'


def get_cpx_interval(x):
    # an example format of CPX_INTERVALS is "DUP_chr1:1499897-1499974"
    type_chr = x.split('_chr')
    chr_pos = type_chr[1].split(':')
    pos = chr_pos[1].split('-')
    return hl.struct(type=type_chr[0], chrom=chr_pos[0], start=hl.int32(pos[0]), end=hl.int32(pos[1]))


class SeqrSVVariantSchema(BaseMTSchema):

    def __init__(self, *args, gene_id_mapping, **kwargs):
        super().__init__(*args, **kwargs)
        self.gene_id_mapping = gene_id_mapping

    @row_annotation()
    def contig(self):
        # 'contig': lambda rows: rows.locus.contig.replace('^chr', ''),
        return variant_id.get_expr_for_contig(self.mt.locus)

    @row_annotation()
    def sc(self):
        return self.mt.info.AC[0]

    @row_annotation()
    def sf(self):
        return self.mt.info.AN[0]

    @row_annotation()
    def sn(self):
        return self.mt.info.AN

    @row_annotation()
    def start(self):
        # 'start': lambda rows: rows.locus.position,
        return variant_id.get_expr_for_start_pos(self.mt)

    @row_annotation()
    def end(self):
        return self.mt.info.END

    @row_annotation(name='sv_callset_Het')
    def sv_callset_het(self):
        return self.mt.info.N_HET

    @row_annotation(name='sv_callset_Hom')
    def sv_callset_hom(self):
        return self.mt.info.H_HOMALT

    @row_annotation(name='gnomad_svs_ID')
    def gnomad_svs_id(self):
        return self.mt.info.gnomAD_V2_SVID

    @row_annotation(name='gnomad_svs_AF')
    def gnomad_svs_af(self):
        return self.mt.info.gnomAD_V2_AF

    @row_annotation(name='gnomad_svs_AC')
    def gnomad_svs_ac(self):
        return self.mt.info.gnomAD_V2_AC_AF

    @row_annotation(name='gnomad_svs_AN')
    def gnomad_svs_an(self):
        return self.mt.info.gnomAD_V2_AN_AF

    @row_annotation()
    def pos(self):
        # 'start': lambda rows: rows.locus.position,
        return variant_id.get_expr_for_start_pos(self.mt)

    @row_annotation()
    def filters(self):
        return self.mt.filters.filter(
            lambda x: (x != PASS) & (x != BOTHSIDES_SUPPORT)
        )

    @row_annotation()
    def bothsides_support(self):
        return self.mt.filters.any(
            lambda x: x == BOTHSIDES_SUPPORT
        )

    @row_annotation()
    def algorithms():
        return self.mt.info.ALGORITHMS

    @row_annotation()
    def xpos(self):
        # 'xpos': lambda rows: get_xpos(rows.locus.contig, rows.locus.position),
        return variant_id.get_expr_for_xpos(self.mt.locus)

    @row_annotation()
    def cpx_intervals(self):
        return hl.if_else(
            hl.is_defined(self.mt.info.CPX_INTERVALS),
            self.mt.info.CPX_INTERVALS.map(lambda x: get_cpx_interval(x)),
            hl.missing(hl.dtype(INTERVAL_TYPE))
        )

    @row_annotation()
    def end_locus(self):
        return hl.if_else(
            hl.is_defined(self.mt.info.END2),
            hl.struct(contig=self.mt.info.CHR2, pos=self.mt.info.END2),
            hl.struct(contig=self.mt.locus.contig, pos=self.mt.info.END)
        ),

    @row_annotation(fn_require=xpos)
    def xstart(self):
        return self.mt.xpos

    @row_annotation(fn_require=end_locus)
    def xstop(self):
        return variant_id.get_expr_for_expos(self.mt.end_locus)


class SeqrSVGenotypesSchema(BaseMTSchema):
    pass

class SeqrSVVariantsAndGenotypesSchema(
    SeqrSVVariantSchema, SeqrSVGenotypesSchema, SeqrVariantsAndGenotypesSchema
):
    pass
