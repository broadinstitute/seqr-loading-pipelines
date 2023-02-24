import datetime

import hail as hl

from lib.model.base_mt_schema import BaseMTSchema, row_annotation, RowAnnotationOmit
from lib.model.seqr_mt_schema import SeqrGenotypesSchema, SeqrVariantsAndGenotypesSchema

from hail_scripts.computed_fields import variant_id

BOOL_MAP = {'TRUE': True, 'FALSE': False}
SAMPLE_ID_REGEX = '(?P<sample_id>.+)_v\d+_Exome_(C|RP-)\d+$'

def get_seqr_sample_id(raw_sample_id):
    """
    Extract the seqr sample ID from the raw dataset sample id

    :param raw_sample_id: dataset sample id
    :return: seqr sample id
    """
    try:
        return re.search(SAMPLE_ID_REGEX, raw_sample_id).group('sample_id')
    except AttributeError:
        raise ValueError(raw_sample_id)

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

    @row_annotation():
    def start(self):
        return hl.agg.min(self.mt.start)

    @row_annotation():
    def end(self):
        return hl.agg.max(self.mt.end)

    @row_annotation()
    def num_exon(self):
        return hl.agg.max(self.mt.genes_any_overlap_totalExons)


class SeqrGCNVGenotypesSchema(SeqrGenotypesSchema):
    
    def _genotype_fields(self):
        return {
            'sample_id': get_seqr_sample_id(self.mt.sample_fix),
            'qs': self.mt.qs,
            'cn': self.mt.cn,
            'defragged': BOOL_MAP[self.mt.defragmented.strip()]
        }

class SeqrGCNVVariantsAndGenotypesSchema(SeqrGCNVVariantSchema, SeqrGCNVGenotypesSchema):
    
    @staticmethod
    def elasticsearch_row(ds):
        return SeqrVariantsAndGenotypesSchema.elasticsearch_row(ds)
