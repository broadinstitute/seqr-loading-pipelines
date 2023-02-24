import datetime

import hail as hl

from lib.model.base_mt_schema import BaseMTSchema, row_annotation, RowAnnotationOmit
from lib.model.seqr_mt_schema import SeqrGenotypesSchema, SeqrVariantsAndGenotypesSchema

from hail_scripts.computed_fields import variant_id

class SeqrGCNVVariantSchema(BaseMTSchema):

    def __init__(self, *args, is_new_joint_call=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._is_new_joint_call = is_new_joint_call

    @row_annotation()
    def contig(self):
        return variant_id.get_expr_for_contig(self.mt.chr)

    @row_annotation()
    def sc(self):
        return self.mt.vac

    @row_annotation()
    def sf(self):
        return self.mt.vaf

    @row_annotation()
    def sn(self):
        return self.mt.vac / self.mt.vaf

    @row_annotation(name='svType')
    def sv_type(self):
        return self.mt.svtype

    @row_annotation(name='StrVCTVRE_score')
    def strvctvre(self):
        return self.mt.strvctvre_score

    @row_annotation(name='variantId')
    def variant_id(self):
        return f'{self.mt.variant_name}_{self.mt.svtype}_{datetime.date.today():%m%d%Y}'


class SeqrGCNVGenotypesSchema(SeqrGenotypesSchema):
    pass

class SeqrGCNVVariantsAndGenotypesSchema(SeqrGCNVVariantSchema, SeqrGCNVGenotypesSchema):
    
    @staticmethod
    def elasticsearch_row(ds):
        return SeqrVariantsAndGenotypesSchema.elasticsearch_row(ds)
