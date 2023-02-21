import hail as hl

from lib.model.base_mt_schema import BaseMTSchema, row_annotation, RowAnnotationOmit
from lib.model.seqr_mt_schema import SeqrVariantsAndGenotypesSchema

from hail_scripts.computed_fields import variant_id


class SeqrSVVariantSchema(BaseMTSchema):

    def __init__(self, *args, gene_id_mapping, **kwargs):
        super().__init__(*args, **kwargs)
        self.gene_id_mapping = gene_id_mapping

    @row_annotation(disable_index=True)
    def contig(self):
        # 'contig': lambda rows: rows.locus.contig.replace('^chr', ''),
        return variant_id.get_expr_for_contig(self.mt.locus)

    @row_annotation(disable_index=True)
    def sc(self):
        return self.mt.info.AC[0]

    @row_annotation(disable_index=True)
    def sf(self):
        return self.mt.info.AN[0]

    @row_annotation(disable_index=True)
    def sn(self):
        return self.mt.info.AN

    @row_annotation(disable_index=True)
    def start(self):
        # 'start': lambda rows: rows.locus.position,
        return variant_id.get_expr_for_start_pos(self.mt)

    @row_annotation(disable_index=True)
    def end(self):
        return self.mt.info.END

    # 'sv_callset_Het': lambda rows: rows.info.N_HET,
    # 'sv_callset_Hom': lambda rows: rows.info.N_HOMALT,
    # 'gnomad_svs_ID': lambda rows: rows.info.gnomAD_V2_SVID,
    # 'gnomad_svs_AF': lambda rows: rows.info.gnomAD_V2_AF,
    # 'gnomad_svs_AC': lambda rows: rows.info.gnomAD_V2_AC_AF,
    # 'gnomad_svs_AN': lambda rows: rows.info.gnomAD_V2_AN_AF,

    @row_annotation(disable_index=True)
    def pos(self):
        # 'start': lambda rows: rows.locus.position,
        return variant_id.get_expr_for_start_pos(self.mt)

    # 'filters': lambda rows: hl.array(rows.filters.filter(lambda x: (x != 'PASS') & (x != BOTHSIDES_SUPPORT))),
    # 'bothsides_support': lambda rows: rows.filters.any(lambda x: x == BOTHSIDES_SUPPORT),
    # 'algorithms': lambda rows: rows.info.ALGORITHMS,

    @row_annotation()
    def xpos(self):
        # 'xpos': lambda rows: get_xpos(rows.locus.contig, rows.locus.position),
        return variant_id.get_expr_for_xpos(self.mt.locus)


class SeqrSVGenotypesSchema(BaseMTSchema):
    pass

class SeqrSVVariantsAndGenotypesSchema(
    SeqrSVVariantSchema, SeqrSVGenotypesSchema, SeqrVariantsAndGenotypesSchema
):
    pass
