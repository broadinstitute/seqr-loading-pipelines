import hail as hl

from lib.model.base_mt_schema import BaseMTSchema, row_annotation, RowAnnotationOmit
from hail_scripts.computed_fields import variant_id
from hail_scripts.computed_fields import vep


class SeqrMitoSchema(BaseMTSchema):

    def __init__(self, *args, ref_data, **kwargs):
        self._ref_data = ref_data

        # See _selected_ref_data
        self._selected_ref_data_cache = None

        super().__init__(*args, **kwargs)

    def set_mt(self, mt):
        super().set_mt(mt)
        # set this to None, and the @property _selected_ref_data
        # can populate it if it gets used after each MT update.
        self._selected_ref_data_cache = None

    @property
    def _selected_ref_data(self):
        """
        Reuse `self._ref_data[self.mt.row_key]` for all annotations.
        We'll use the @property _selected_ref_data to access this
        value, and lazily cache it (so as not to compute it if it
        doesn't get get accessed after an update to self.mt).

        Returns: self._ref_data[self.mt.row_key]
        """
        if not self._selected_ref_data_cache:
            self._selected_ref_data_cache = self._ref_data[self.mt.row_key]
        return self._selected_ref_data_cache

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
    def common_low_heteroplasmy(self):
        return self.mt.common_low_heteroplasmy

    # @row_annotation()
    # def originalAltAlleles(self):
        # TODO: This assumes we annotate `locus_old` in this code because `split_multi_hts` drops the proper `old_locus`.
        # If we can get it to not drop it, we should revert this to `old_locus`
    #     return variant_id.get_expr_for_variant_ids(self.mt.locus_old, self.mt.alleles_old)

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
    def gnomad(self):
        return self._selected_ref_data.gnomad_mito

    @row_annotation()
    def mitomap(self):
        return self._selected_ref_data.mitomap

    @row_annotation(name='APOGEE_score')
    def mitimpact(self):
        return self._selected_ref_data.mitimpact

    @row_annotation()
    def hmtvar(self):
        return self._selected_ref_data.hmtvar

    @row_annotation()
    def helix(self):
        return self._selected_ref_data.helix_mito

    @row_annotation()
    def clinvar(self):
        return self._selected_ref_data.clinvar_mito

    @row_annotation()
    def dbnsfp(self):
        return self._selected_ref_data.dbnsfp_mito


class SeqrMitoVariantSchema(SeqrMitoSchema):

    @row_annotation(name='AC')
    def ac(self):
        return self.mt.AC_hom

    @row_annotation(name='AF')
    def af(self):
        return self.mt.AF_hom

    @row_annotation(name='AN')
    def an(self):
        return self.mt.AN


class SeqrMitoGenotypesSchema(BaseMTSchema):

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
    def samples_hl(self, start=0, end=45, step=5):
        # struct of x_to_y to a set of samples in range of x and y for ab.
        return hl.struct(**{
            '%i_to_%i' % (i, i+step): self._genotype_filter_samples(
                lambda g: ((g.num_alt == 1) & ((g.hl*100) >= i) & ((g.hl*100) < i+step))
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
            'num_alt': hl.cond(is_called, hl.cond(self.mt.HL>=0.95, 2, hl.cond(self.mt.HL>=0.01, 1, 0)), -1),
            'gq': hl.cond(is_called, self.mt.MQ, 0),
            'hl': hl.cond(is_called, self.mt.HL, 0),
            'mito_cn': self.mt.mito_cn,
            'contamination': self.mt.contamination,
            'dp': hl.cond(is_called, hl.int(hl.min(self.mt.DP, 32000)), hl.null(hl.tfloat)),
            'sample_id': self.mt.s
        }


class SeqrMitoVariantsAndGenotypesSchema(SeqrMitoVariantSchema, SeqrMitoGenotypesSchema):
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
