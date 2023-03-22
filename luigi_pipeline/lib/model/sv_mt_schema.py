import hail as hl

from luigi_pipeline.lib.model.base_mt_schema import row_annotation, RowAnnotationOmit
from luigi_pipeline.lib.model.seqr_mt_schema import BaseVariantSchema, SeqrGenotypesSchema, SeqrVariantsAndGenotypesSchema

from hail_scripts.computed_fields import variant_id


BOTHSIDES_SUPPORT = "BOTHSIDES_SUPPORT"
GENE_SYMBOL = "gene_symbol"
GENE_ID = "gene_id"
MAJOR_CONSEQUENCE = "major_consequence"
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


class SeqrSVVariantSchema(BaseVariantSchema):

    def __init__(self, *args, gene_id_mapping=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._gene_id_mapping = gene_id_mapping

    def sv_types(self):
        return self.mt.alleles[1].replace('[<>]', '').split(':', 2)

    @row_annotation()
    def sc(self):
        return self.mt.info.AC[0]

    @row_annotation()
    def sf(self):
        return self.mt.info.AF[0]

    @row_annotation()
    def sn(self):
        return self.mt.info.AN

    @row_annotation()
    def end(self):
        return self.mt.info.END

    @row_annotation(name='sv_callset_Het')
    def sv_callset_het(self):
        return self.mt.info.N_HET

    @row_annotation(name='sv_callset_Hom')
    def sv_callset_hom(self):
        return self.mt.info.N_HOMALT

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

    @row_annotation(name='StrVCTVRE_score')
    def strvctvre(self):
        return self.mt.info.StrVCTVRE

    @row_annotation()
    def filters(self):
        filters = self.mt.filters.filter(
            lambda x: (x != PASS) & (x != BOTHSIDES_SUPPORT)
        )
        return hl.or_missing(filters.size() > 0, filters)

    @row_annotation(disable_index=True)
    def bothsides_support(self):
        return self.mt.filters.any(
            lambda x: x == BOTHSIDES_SUPPORT
        )

    @row_annotation(disable_index=True)
    def algorithms(self):
        return self.mt.info.ALGORITHMS

    @row_annotation(disable_index=True)
    def cpx_intervals(self):
        return hl.or_missing(
            hl.is_defined(self.mt.info.CPX_INTERVALS),
            self.mt.info.CPX_INTERVALS.map(lambda x: get_cpx_interval(x)),
        )

    @row_annotation(disable_index=True)
    def end_locus(self):
        return hl.if_else(
            hl.is_defined(self.mt.info.END2),
            hl.struct(contig=self.mt.info.CHR2, position=self.mt.info.END2),
            hl.struct(contig=self.mt.locus.contig, position=self.mt.info.END)
        )

    @row_annotation(name='sortedTranscriptConsequences')
    def sorted_transcript_consequences(self):
        conseq_predicted_gene_cols = [
            gene_col for gene_col in self.mt.info if gene_col.startswith(CONSEQ_PREDICTED_PREFIX)
            and gene_col not in NON_GENE_PREDICTIONS
        ]
        mapped_genes = [
            self.mt.info[gene_col].map(
                lambda gene: hl.struct(**{
                    GENE_SYMBOL: gene,
                    GENE_ID: self._gene_id_mapping.get(gene, hl.missing(hl.tstr)),
                    MAJOR_CONSEQUENCE: gene_col.replace(CONSEQ_PREDICTED_PREFIX, '', 1)
                })
            )
            for gene_col in conseq_predicted_gene_cols
        ]
        return hl.filter(
            hl.is_defined,
            mapped_genes
        ).flatmap(lambda x: x)

    @row_annotation(fn_require=end_locus)
    def xstop(self):
        return variant_id.get_expr_for_xpos(self.mt.end_locus)

    # rg37_locus is annotated by HailMatrixTableTask.add_37_coordinates

    @row_annotation(fn_require=end_locus)
    def rg37_locus_end(self):
        return hl.or_missing(
            self.mt.end_locus.position <= hl.literal(hl.get_reference('GRCh38').lengths)[self.mt.end_locus.contig],
            hl.liftover(hl.locus(self.mt.end_locus.contig, self.mt.end_locus.position, reference_genome='GRCh38'), 'GRCh37'),
        )

    @row_annotation(name='svType')
    def sv_type(self):
        return self.sv_types()[0]

    @row_annotation(name='transcriptConsequenceTerms', fn_require=[
        sorted_transcript_consequences, sv_type,
    ])
    def transcript_consequence_terms(self):
        return self.mt.sortedTranscriptConsequences.map(lambda x: x[MAJOR_CONSEQUENCE]).extend([self.mt.svType])

    @row_annotation()
    def sv_type_detail(self):
        sv_types = self.sv_types()
        return hl.if_else(
            sv_types[0] == 'CPX',
            self.mt.info.CPX_TYPE,
            hl.or_missing(
                (sv_types[0] == 'INS') & (hl.len(sv_types) > 1),
                sv_types[1],
            )
        )

    @row_annotation(name='geneIds', fn_require=sorted_transcript_consequences)
    def gene_ids(self):
        return hl.set(
            self.mt.sortedTranscriptConsequences.filter(
                lambda x: x[MAJOR_CONSEQUENCE] != 'NEAREST_TSS'
            ).map(
                lambda x: x[GENE_ID]
            )
        )

    @row_annotation(name='variantId', disable_index=True)
    def variant_id(self):
        return self.mt.rsid

    # NB: This is the "elasticsearch_mapping_id" used inside of export_table_to_elasticsearch.
    @row_annotation(name='docId', disable_index=True)
    def doc_id(self, max_length=512):
        return self.mt.rsid[0: max_length]


class SeqrSVGenotypesSchema(SeqrGenotypesSchema):

    def _genotype_fields(self):
        is_called = hl.is_defined(self.mt.GT)
        return {
            'sample_id': self.mt.s,
            'gq': self.mt.GQ,
            'cn': self.mt.RD_CN,
            'num_alt': self._num_alt(is_called)
        }

    # NB: override this function here to mimic the existing null handling behavior.
    def _genotype_filter_samples(self, filter_):
        samples = super()._genotype_filter_samples(filter_)
        return hl.or_missing(hl.len(samples) > 0, samples)            

    @row_annotation(name="samples_gq_sv", fn_require=SeqrGenotypesSchema.genotypes)
    def samples_gq(self):
        # NB: super().samples_gq is a RowAnnotation... so we call the method under the hood.
        # ew it is gross.
        return super().samples_gq.fn(self, start=0, end=1000, step=10)

    def samples_ab(self):
        pass

class SeqrSVVariantsAndGenotypesSchema(SeqrSVVariantSchema, SeqrSVGenotypesSchema):
    
    @staticmethod
    def elasticsearch_row(ds):
        return SeqrVariantsAndGenotypesSchema.elasticsearch_row(ds)
