import hail as hl

from lib.model.base_mt_schema import BaseMTSchema, row_annotation, RowAnnotationOmit
from hail_scripts.computed_fields import variant_id

class UpdateSchema(BaseMTSchema):
    @row_annotation(name='docId')
    def doc_id(self, length=512):
        return variant_id.get_expr_for_variant_id(self.mt, length)

    @row_annotation()
    def aIndex(self):
        return self.mt.a_index

class HGMDSchema(UpdateSchema):

    def __init__(self, *args, hgmd_data, **kwargs):
        super().__init__(*args, **kwargs)
        self._hgmd_data = hgmd_data

    @row_annotation()
    def hgmd(self):
        if self._hgmd_data is None:
            raise RowAnnotationOmit
        return hl.struct(**{'accession': self._hgmd_data[self.mt.row_key].rsid,
                            'class': self._hgmd_data[self.mt.row_key].info.CLASS})


class CLINVARSchema(UpdateSchema):

    def __init__(self, *args, clinvar_data, **kwargs):
        super().__init__(*args, **kwargs)
        self._clinvar_data = clinvar_data

    @row_annotation()
    def clinvar(self):
        return hl.struct(**{'allele_id': self._clinvar_data[self.mt.row_key].info.ALLELEID,
                            'clinical_significance': hl.delimit(self._clinvar_data[self.mt.row_key].info.CLNSIG),
                            'gold_stars': self._clinvar_data[self.mt.row_key].gold_stars})


class CIDRSchema(UpdateSchema):

    def __init__(self, *args, cidr_data, **kwargs):
        super().__init__(*args, **kwargs)
        self._cidr_data = cidr_data

    @row_annotation()
    def cidr(self):
        if self._cidr_data is None:
            raise RowAnnotationOmit
        return hl.struct(**{'AC': self._cidr_data[self.mt.row_key].info.AC[self.mt.a_index-1],
                            'AF': self._cidr_data[self.mt.row_key].info.AF[self.mt.a_index-1]})
