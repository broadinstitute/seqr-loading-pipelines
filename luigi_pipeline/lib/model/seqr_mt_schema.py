
import hail as hl

from lib.model.base_mt_schema import BaseMTSchema, mt_annotation
from hail_scripts.v02.utils.computed_fields import variant_id
from hail_scripts.v02.utils.computed_fields import vep


class SeqrSchema(BaseMTSchema):

    @mt_annotation(annotation='sortedTranscriptConsequences')
    def sorted_transcript_consequences(self):
        return vep.get_expr_for_vep_sorted_transcript_consequences_array(self.mt.vep)

    @mt_annotation(annotation='docId', fn_require=sorted_transcript_consequences)
    def doc_id(self, length=512):
        return variant_id.get_expr_for_variant_id(self.mt, length)

    @mt_annotation(annotation='variantId', fn_require=sorted_transcript_consequences)
    def variant_id(self):
        return variant_id.get_expr_for_variant_id(self.mt)

    @mt_annotation(fn_require=sorted_transcript_consequences)
    def contig(self):
        return variant_id.get_expr_for_contig(self.mt.locus)

    @mt_annotation(fn_require=sorted_transcript_consequences)
    def pos(self):
        return variant_id.get_expr_for_start_pos(self.mt)

    @mt_annotation(fn_require=sorted_transcript_consequences)
    def start(self):
        return variant_id.get_expr_for_start_pos(self.mt)

    @mt_annotation(fn_require=sorted_transcript_consequences)
    def end(self):
        return variant_id.get_expr_for_end_pos(self.mt)

    @mt_annotation(fn_require=sorted_transcript_consequences)
    def ref(self):
        return variant_id.get_expr_for_ref_allele(self.mt)

    @mt_annotation(fn_require=sorted_transcript_consequences)
    def alt(self):
        return variant_id.get_expr_for_alt_allele(self.mt)

    @mt_annotation(fn_require=sorted_transcript_consequences)
    def xpos(self):
        return variant_id.get_expr_for_xpos(self.mt.locus)

    @mt_annotation(fn_require=sorted_transcript_consequences)
    def xstart(self):
        return variant_id.get_expr_for_xpos(self.mt.locus)

    @mt_annotation(fn_require=sorted_transcript_consequences)
    def xstop(self):
        return variant_id.get_expr_for_xpos(self.mt.locus) + hl.len(variant_id.get_expr_for_ref_allele(self.mt))

    @mt_annotation(fn_require=sorted_transcript_consequences)
    def domains(self):
        return vep.get_expr_for_vep_protein_domains_set_from_sorted(
            self.mt.sortedTranscriptConsequences)

    @mt_annotation(annotation='transcriptConsequenceTerms', fn_require=sorted_transcript_consequences)
    def transcript_consequence_terms(self):
        return vep.get_expr_for_vep_consequence_terms_set(self.mt.sortedTranscriptConsequences)

    @mt_annotation(annotation='transcriptIds', fn_require=sorted_transcript_consequences)
    def transcript_ids(self):
        return vep.get_expr_for_vep_transcript_ids_set(self.mt.sortedTranscriptConsequences)

    @mt_annotation(annotation='mainTranscript', fn_require=sorted_transcript_consequences)
    def main_transcript(self):
        return vep.get_expr_for_worst_transcript_consequence_annotations_struct(
            self.mt.sortedTranscriptConsequences)

    @mt_annotation(annotation='geneIds', fn_require=sorted_transcript_consequences)
    def gene_ids(self):
        return vep.get_expr_for_vep_gene_ids_set(self.mt.sortedTranscriptConsequences)

    @mt_annotation(annotation='codingGeneIds', fn_require=sorted_transcript_consequences)
    def coding_gene_ids(self):
        return vep.get_expr_for_vep_gene_ids_set(self.mt.sortedTranscriptConsequences, only_coding_genes=True)


class SeqrVariantSchema(SeqrSchema):

    @mt_annotation(annotation='AC')
    def ac(self):
        return self.mt.info.AC

    @mt_annotation(annotation='AF')
    def af(self):
        return self.mt.info.AF

    @mt_annotation(annotation='AN')
    def an(self):
        return self.mt.info.AN


class SeqrSVSchema(SeqrSchema):

    @mt_annotation(annotation='IMPRECISE')
    def imprecise(self):
        return self.mt.info.IMPRECISE

    @mt_annotation(annotation='SVTYPE')
    def svtype(self):
        return self.mt.info.SVTYPE

    @mt_annotation(annotation='SVLEN')
    def svlen(self):
        return self.mt.info.SVLEN

    @mt_annotation(annotation='END')
    def end(self):
        return self.mt.info.END
