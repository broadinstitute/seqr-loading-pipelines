
import hail as hl

from lib.model.base_mt_schema import BaseMTSchema, row_annotation, RowAnnotationOmit
from hail_scripts.v02.utils.computed_fields import variant_id
from hail_scripts.v02.utils.computed_fields import vep


class SeqrSchema(BaseMTSchema):

    def __init__(self, *args, ref_data, clinvar_data, hgmd_data=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._ref_data = ref_data
        self._clinvar_data = clinvar_data
        self._hgmd_data = hgmd_data

    @row_annotation()
    def vep(self):
        return self.mt.vep

    @row_annotation()
    def rsid(self):
        return self.mt.rsid

    @row_annotation()
    def filters(self):
        return self.mt.filters

    @row_annotation()
    def aIndex(self):
        return self.mt.a_index

    @row_annotation()
    def originalAltAlleles(self):
        # TODO: This assumes we annotate `locus_old` in this code because `split_multi_hts` drops the proper `old_locus`.
        # If we can get it to not drop it, we should revert this to `old_locus`
        return variant_id.get_expr_for_variant_ids(self.mt.locus_old, self.mt.alleles_old)

    @row_annotation(name='sortedTranscriptConsequences', fn_require=vep)
    def sorted_transcript_consequences(self):
        return vep.get_expr_for_vep_sorted_transcript_consequences_array(self.mt.vep)

    @row_annotation(name='docId')
    def doc_id(self, length=512):
        return variant_id.get_expr_for_variant_id(self.mt, length)

    @row_annotation(name='variantId')
    def variant_id(self):
        return variant_id.get_expr_for_variant_id(self.mt)

    @row_annotation()
    def contig(self):
        return variant_id.get_expr_for_contig(self.mt.locus)

    @row_annotation()
    def pos(self):
        return variant_id.get_expr_for_start_pos(self.mt)

    @row_annotation()
    def start(self):
        return variant_id.get_expr_for_start_pos(self.mt)

    @row_annotation()
    def end(self):
        return variant_id.get_expr_for_end_pos(self.mt)

    @row_annotation()
    def ref(self):
        return variant_id.get_expr_for_ref_allele(self.mt)

    @row_annotation()
    def alt(self):
        return variant_id.get_expr_for_alt_allele(self.mt)

    @row_annotation()
    def xpos(self):
        return variant_id.get_expr_for_xpos(self.mt.locus)

    @row_annotation()
    def xstart(self):
        return variant_id.get_expr_for_xpos(self.mt.locus)

    @row_annotation()
    def xstop(self):
        return variant_id.get_expr_for_xpos(self.mt.locus) + hl.len(variant_id.get_expr_for_ref_allele(self.mt)) - 1

    @row_annotation(fn_require=sorted_transcript_consequences)
    def domains(self):
        return vep.get_expr_for_vep_protein_domains_set_from_sorted(
            self.mt.sortedTranscriptConsequences)

    @row_annotation(name='transcriptConsequenceTerms', fn_require=sorted_transcript_consequences)
    def transcript_consequence_terms(self):
        return vep.get_expr_for_vep_consequence_terms_set(self.mt.sortedTranscriptConsequences)

    @row_annotation(name='transcriptIds', fn_require=sorted_transcript_consequences)
    def transcript_ids(self):
        return vep.get_expr_for_vep_transcript_ids_set(self.mt.sortedTranscriptConsequences)

    @row_annotation(name='mainTranscript', fn_require=sorted_transcript_consequences)
    def main_transcript(self):
        return vep.get_expr_for_worst_transcript_consequence_annotations_struct(
            self.mt.sortedTranscriptConsequences)

    @row_annotation(name='geneIds', fn_require=sorted_transcript_consequences)
    def gene_ids(self):
        return vep.get_expr_for_vep_gene_ids_set(self.mt.sortedTranscriptConsequences)

    @row_annotation(name='codingGeneIds', fn_require=sorted_transcript_consequences)
    def coding_gene_ids(self):
        return vep.get_expr_for_vep_gene_ids_set(self.mt.sortedTranscriptConsequences, only_coding_genes=True)

    @row_annotation()
    def cadd(self):
        return self._ref_data[self.mt.row_key].cadd

    @row_annotation()
    def dbnsfp(self):
        return self._ref_data[self.mt.row_key].dbnsfp

    @row_annotation()
    def geno2mp(self):
        return self._ref_data[self.mt.row_key].geno2mp

    @row_annotation()
    def gnomad_exomes(self):
        return self._ref_data[self.mt.row_key].gnomad_exomes

    @row_annotation()
    def gnomad_exome_coverage(self):
        return self._ref_data[self.mt.row_key].gnomad_exome_coverage

    @row_annotation()
    def gnomad_genomes(self):
        return self._ref_data[self.mt.row_key].gnomad_genomes

    @row_annotation()
    def gnomad_genome_coverage(self):
        return self._ref_data[self.mt.row_key].gnomad_genome_coverage

    @row_annotation()
    def eigen(self):
        return self._ref_data[self.mt.row_key].eigen

    @row_annotation()
    def exac(self):
        return self._ref_data[self.mt.row_key].exac

    @row_annotation()
    def g1k(self):
        return self._ref_data[self.mt.row_key].g1k

    @row_annotation()
    def mpc(self):
        return self._ref_data[self.mt.row_key].mpc

    @row_annotation()
    def primate_ai(self):
        return self._ref_data[self.mt.row_key].primate_ai

    @row_annotation()
    def splice_ai(self):
        return self._ref_data[self.mt.row_key].splice_ai

    @row_annotation()
    def topmed(self):
        return self._ref_data[self.mt.row_key].topmed

    @row_annotation()
    def hgmd(self):
        if self._hgmd_data is None:
            raise RowAnnotationOmit
        return hl.struct(**{'accession': self._hgmd_data[self.mt.row_key].rsid,
                            'class': self._hgmd_data[self.mt.row_key].info.CLASS})

    @row_annotation()
    def clinvar(self):
        return hl.struct(**{'allele_id': self._clinvar_data[self.mt.row_key].info.ALLELEID,
                            'clinical_significance': hl.delimit(self._clinvar_data[self.mt.row_key].info.CLNSIG),
                            'gold_stars': self._clinvar_data[self.mt.row_key].gold_stars})


class SeqrVariantSchema(SeqrSchema):

    @row_annotation(name='AC')
    def ac(self):
        return self.mt.info.AC[self.mt.a_index-1]

    @row_annotation(name='AF')
    def af(self):
        return self.mt.info.AF[self.mt.a_index-1]

    @row_annotation(name='AN')
    def an(self):
        return self.mt.info.AN


class SeqrGenotypesSchema(BaseMTSchema):

    @row_annotation()
    def genotypes(self):
        return hl.agg.collect(hl.struct(**self._genotype_fields()))

    @row_annotation(fn_require=genotypes)
    def samples_no_call(self):
        return self._genotype_filter_samples(lambda g: g.num_alt == -1)

    @row_annotation(fn_require=genotypes)
    def samples_num_alt(self, start=1, end=3, step=1):
        return hl.struct(**{
            '%i' % i: self._genotype_filter_samples(lambda g: g.num_alt == i)
            for i in range(start, end, step)
        })

    @row_annotation(fn_require=genotypes)
    def samples_gq(self, start=0, end=95, step=5):
        # struct of x_to_y to a set of samples in range of x and y for gq.
        return hl.struct(**{
            '%i_to_%i' % (i, i+step): self._genotype_filter_samples(lambda g: ((g.gq >= i) & (g.gq < i+step)))
            for i in range(start, end, step)
        })

    @row_annotation(fn_require=genotypes)
    def samples_ab(self, start=0, end=45, step=5):
        # struct of x_to_y to a set of samples in range of x and y for ab.
        return hl.struct(**{
            '%i_to_%i' % (i, i+step): self._genotype_filter_samples(
                lambda g: ((g.num_alt == 1) & ((g.ab*100) >= i) & ((g.ab*100) < i+step))
            )
            for i in range(start, end, step)
        })

    def _genotype_filter_samples(self, filter):
        # Filter on the genotypes.
        return hl.set(self.mt.genotypes.filter(filter).map(lambda g: g.sample_id))

    def _genotype_fields(self):
        # Convert the mt genotype entries into num_alt, gq, ab, dp, and sample_id.
        is_called = hl.is_defined(self.mt.GT)
        return {
            'num_alt': hl.cond(is_called, self.mt.GT.n_alt_alleles(), -1),
            'gq': hl.cond(is_called, self.mt.GQ, hl.null(hl.tint)),
            'ab': hl.bind(
                lambda total: hl.cond((is_called) & (total != 0) & (hl.len(self.mt.AD) > 1),
                                      hl.float(self.mt.AD[1] / total),
                                      hl.null(hl.tfloat)),
                hl.sum(self.mt.AD)
            ),
            'dp': hl.cond(is_called, hl.int(hl.min(self.mt.DP, 32000)), hl.null(hl.tfloat)),
            'sample_id': self.mt.s
        }


class SeqrVariantsAndGenotypesSchema(SeqrVariantSchema, SeqrGenotypesSchema):
    """
    Combined variant and genotypes.
    """

    @staticmethod
    def elasticsearch_row(ds):
        """
        Prepares the mt to export using ElasticsearchClient V02.
        - Flattens nested structs
        - drops locus and alleles key

        TODO:
        - Call validate
        - when all annotations are here, whitelist fields to send instead of blacklisting.
        :return:
        """
        # Converts a mt to the row equivalent.
        if isinstance(ds, hl.MatrixTable):
            ds = ds.rows()
        # Converts nested structs into one field, e.g. {a: {b: 1}} => a.b: 1
        table = ds.drop('vep').flatten()
        # When flattening, the table is unkeyed, which causes problems because our locus and alleles should not
        # be normal fields. We can also re-key, but I believe this is computational?
        table = table.drop(table.locus, table.alleles)


        return table
