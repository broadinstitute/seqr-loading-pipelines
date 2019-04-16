
import hail as hl

from lib.model.base_mt_schema import BaseMTSchema, row_annotation
from hail_scripts.v02.utils.computed_fields import variant_id
from hail_scripts.v02.utils.computed_fields import vep


class SeqrSchema(BaseMTSchema):

    @row_annotation()
    def vep(self):
        return self.mt.vep

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
        return variant_id.get_expr_for_xpos(self.mt.locus) + hl.len(variant_id.get_expr_for_ref_allele(self.mt))

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


class SeqrVariantSchema(SeqrSchema):

    @row_annotation(name='AC')
    def ac(self):
        return self.mt.info.AC

    @row_annotation(name='AF')
    def af(self):
        return self.mt.info.AF

    @row_annotation(name='AN')
    def an(self):
        return self.mt.info.AN


class SeqrSVSchema(SeqrSchema):

    @row_annotation(name='IMPRECISE')
    def imprecise(self):
        return self.mt.info.IMPRECISE

    @row_annotation(name='SVTYPE')
    def svtype(self):
        return self.mt.info.SVTYPE

    @row_annotation(name='SVLEN')
    def svlen(self):
        return self.mt.info.SVLEN

    @row_annotation(name='END')
    def end(self):
        return self.mt.info.END
