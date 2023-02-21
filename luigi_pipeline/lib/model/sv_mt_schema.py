import hail as hl

from lib.model.base_mt_schema import BaseMTSchema, row_annotation, RowAnnotationOmit
from lib.model.seqr_mt_schema import SeqrVariantsAndGenotypesSchema

from hail_scripts.computed_fields import variant_id


BOTHSIDES_SUPPORT = "BOTHSIDES_SUPPORT"
PASS = "PASS"

# Used to filter mt.info fields.
CONSEQ_PREDICTED_PREFIX = 'PREDICTED_'
NON_GENE_PREDICTIONS = {'PREDICTED_INTERGENIC', 'PREDICTED_NONCODING_BREAKPOINT', 'PREDICTED_NONCODING_SPAN'}


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
        filters = hl.array(self.mt.filters.filter(
            lambda x: (x != PASS) & (x != BOTHSIDES_SUPPORT)
        ))
        return hl.if_else(
            hl.len(filters) > 0,
            filters,
            hl.missing(hl.dtype('array<str>')),
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

    @row_annotation(disable_index=True)
    def end_locus(self):
        return hl.if_else(
            hl.is_defined(self.mt.info.END2),
            hl.struct(contig=self.mt.info.CHR2, pos=self.mt.info.END2),
            hl.struct(contig=self.mt.locus.contig, pos=self.mt.info.END)
        )

    @row_annotation(name='sortedTranscriptConsequences')
    def sorted_transcript_consequences(self):
        # NB: this function is nuts plz help.
        conseq_predicted_gene_cols = [
            gene_col for gene_col in self.mt.info if gene_col.startswith(CONSEQ_PREDICTED_PREFIX)
            and gene_col not in NON_GENE_PREDICTIONS
        ]
        mapped_genes = [
            rows.info[gene_col].map(
                lambda gene: hl.struct(**{
                    'gene_symbol': gene,
                    'gene_id': self.gene_id_mapping[gene], 
                    'major_consequence': gene_col.replace(CONSEQ_PREDICTED_PREFIX, '', 1)
                })
            )
            for gene_col in conseq_predicted_gene_cols
        ]
        return hl.flatmap(
            lambda x: x, 
            hl.filter(
                lambda x: hl.is_defined(x),
                mapped_genes,
            )
        )

    @row_annotation(fn_require=xpos)
    def xstart(self):
        return self.mt.xpos

    @row_annotation(fn_require=end_locus)
    def xstop(self):
        return variant_id.get_expr_for_xpos(self.mt.end_locus)

    # rg37_locus is annotated by HailMatrixTableTask.add_37_coordinates

    @row_annotation(fn_require=end_locus)
    def rg37_locus_end(self):
        return hl.if_else(
            self.mt.end_locus <= hl.literal(hl.get_reference('GRCh38').lengths)[self.mt.end_locus.contig],
            hl.liftover(hl.locus(self.mt.end_locus.contig, self.mt.end_locus.pos, reference_genome='GRCh38'), 'GRCh37'),
            hl.missing('locus<GRCh37>')
        )

    @row_annotation(name='svType')
    def sv_type(self):
        return self.mt.alleles[1].replace('[<>]', '').split(':', 2)[0]

    @row_annotation(name='transcriptConsequenceTerms', fn_require=[
        sorted_transcript_consequences, sv_type,
    ])
    def transcript_consequence_terms(self):
        return self.mt.sortedTranscriptConsequences.map(lambda x: x.major_consequence).extend([self.mt.svType])

    @row_annotation()
    def sv_type_detail(self):
        sv_types = self.mt.alleles[1].replace('[<>]', '').split(':', 2)
        return hl.if_else(
            sv_types[0] == 'CPX',
            self.mt.info.CPX_TYPE,
            hl.if_else(
                (sv_types[0] == 'INS') & (hl.len(sv_types) > 1),
                sv_types[1],
                hl.missing('str')
            )
        )

    @row_annotation(name='geneIds', fn_require=sorted_transcript_consequences)
    def gene_ids(self):
        return hl.set(
            hl.map(
                lambda x: x.gene_id, self.mt.sortedTranscriptConsequences.filter(
                    lambda x: x.major_consequence != 'NEAREST_TSS'
                )
            )
        )

    @row_annotation(name='variantId')
    def variant_id(self):
        return self.mt.rsid

    # NB: This is the "elasticsearch_mapping_id" used inside of export_table_to_elasticsearch.
    @row_annotation(name='docId', disable_index=True)
    def doc_id(self, max_length=512):
        return self.mt.rsid[0: max_length]


class SeqrSVGenotypesSchema(BaseMTSchema):

    @row_annotation()
    def genotypes(self):
        return hl.agg.collect(hl.struct(**self._genotype_fields()))
    
    def _genotype_fields(self):
        # Convert the mt genotype entries into num_alt, gq, hl, mito_cn, contamination, dp, and sample_id.
        is_called = hl.is_defined(self.mt.GT)
        return {
            'sample_id': self.mt.s,
            'gq': self.mt.GQ,
            'cn': self.mt.RD_CN,
            'num_alt': hl.if_else(is_called, self.mt.GT.n_alt_alleles(), -1)
        }

class SeqrSVVariantsAndGenotypesSchema(
    SeqrSVVariantSchema, SeqrSVGenotypesSchema, SeqrVariantsAndGenotypesSchema
):
    pass
