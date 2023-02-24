import hail as hl

from lib.model.base_mt_schema import BaseMTSchema, row_annotation, RowAnnotationOmit
from lib.model.seqr_mt_schema import SeqrGenotypesSchema, SeqrVariantsAndGenotypesSchema

class SeqrGCNVVariantSchema(BaseMTSchema):

    def __init__(self, *args, is_new_joint_call=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._is_new_joint_call = is_new_joint_call


class SeqrSVGenotypesSchema(SeqrGenotypesSchema):

class SeqrSVVariantsAndGenotypesSchema(SeqrSVVariantSchema, SeqrSVGenotypesSchema):
    
    @staticmethod
    def elasticsearch_row(ds):
        return SeqrVariantsAndGenotypesSchema.elasticsearch_row(ds)
