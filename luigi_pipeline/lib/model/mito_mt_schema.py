import hail as hl

from lib.model.base_mt_schema import row_annotation
from lib.model.seqr_mt_schema import BaseSeqrSchema, SeqrGenotypesSchema, SeqrVariantsAndGenotypesSchema


class SeqrMitoVariantSchema(BaseSeqrSchema):

    def __init__(self, *args, high_constraint_region, **kwargs):
        super().__init__(*args, **kwargs)
        self._high_constraint_region = high_constraint_region

    # Mitochondrial only fields
    @row_annotation()
    def gnomad_mito(self):
        return self._selected_ref_data.gnomad_mito

    @row_annotation()
    def mitomap(self):
        return self._selected_ref_data.mitomap

    @row_annotation(name='APOGEE')
    def mitimpact(self):
        return self._selected_ref_data.mitimpact

    @row_annotation(name='HmtVar')
    def hmtvar(self):
        return self._selected_ref_data.hmtvar

    @row_annotation()
    def helix(self):
        return self._selected_ref_data.helix_mito

    @row_annotation()
    def common_low_heteroplasmy(self):
        return self.mt.common_low_heteroplasmy

    @row_annotation()
    def hap_defining_variant(self):
        return self.mt.hap_defining_variant

    @row_annotation()
    def mitotip_trna_prediction(self):
        return self.mt.mitotip_trna_prediction

    @row_annotation()
    def high_constraint_region(self):
        return hl.is_defined(self._high_constraint_region[self.mt.locus])

    @row_annotation(name='AC_het')
    def ac_het(self):
        return self.mt.AC_het

    @row_annotation(name='AF_het')
    def af_het(self):
        return self.mt.AF_het

    # Fields with the same names but annotated differently
    @row_annotation()
    def rsid(self):
        return self.mt.rsid.find(lambda x: hl.is_defined(x))

    @row_annotation(name='AC')
    def ac(self):
        return self.mt.AC_hom

    @row_annotation(name='AF')
    def af(self):
        return self.mt.AF_hom

    @row_annotation(name='AN')
    def an(self):
        return self.mt.AN


class SeqrMitoGenotypesSchema(SeqrGenotypesSchema):

    @row_annotation(fn_require=SeqrGenotypesSchema.genotypes)
    def samples_hl(self, start=0, end=45, step=5):
        # struct of x_to_y to a set of samples in range of x and y for heteroplasmy level.
        return hl.struct(**{
            '%i_to_%i' % (i, i+step): self._genotype_filter_samples(
                lambda g: ((g.num_alt == 1) & ((g.hl*100) >= i) & ((g.hl*100) < i+step))
            )
            for i in range(start, end, step)
        })

    # Override the samples_ab annotation
    def samples_ab(self):
        pass

    def _genotype_fields(self):
        # Convert the mt genotype entries into num_alt, gq, hl, mito_cn, contamination, dp, and sample_id.
        is_called = hl.is_defined(self.mt.GT)
        return {
            'num_alt': hl.cond(is_called, hl.cond(self.mt.HL>=0.95, 2, hl.cond(self.mt.HL>=0.01, 1, 0)), -1),
            'gq': hl.cond(is_called, self.mt.MQ, 0),
            'hl': hl.cond(is_called, self.mt.HL, 0),
            'mito_cn': hl.int(self.mt.mito_cn),
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
        return SeqrVariantsAndGenotypesSchema.elasticsearch_row(ds)
