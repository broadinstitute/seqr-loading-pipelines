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


def parse_genes(gen_col):
    return hl.agg.collect_as_set(
        gene.split('.')[0] for gene in gene_col.split(',') 
        if gene not in {'None', 'null', 'NA', ''}
    )

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
        return hl.if_else(
            hl.is_defined(self.mt.vaf),
            self.mt.vac / self.mt.vaf,
            hl.missing(hl.tint32)
        )

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

    @row_annotation(name='geneIds')
    def gene_ids(self):
        return list(parse_genes(self.mt.genes_any_overlap_Ensemble_ID))

    @row_annotation(name='transcriptConsequenceTerms', fn_require=sv_type)
    def transcript_consequence_terms(self):
        sv_type = {'gCNV_{}'.format(self.mt.svType)}
        
        if hl.len(parse_genes(self.mt.genes_LOF_Ensemble_ID)):
            sv_type.add("LOF")

        if hl.len(parse_genes(self.mt.genes_CG_Ensemble_ID)):
            sv_type.add("COPY_GAIN")

        return list(sv_type)

    @row_annotation(name='sortedTranscriptConsequences', fn_require=gene_ids)
    def sorted_transcript_consequences(self):
        lof_genes = parse_genes(self.mt.genes_LOF_Ensemble_ID)
        copy_gain_genes = parse_genes(self.mt.genes_CG_Ensemble_ID)
        major_consequence_genes = lof_genes | copy_gain_genes
        return [
            {
                "gene_id": gene, 
                "major_consequence": "LOF" if gene in lof_genes else "COPY_GAIN"
            }
            if gene in major_consequence_genes else {"gene_id": gene}
            for gene in self.mt.geneIds
        ]

    @row_annotation(fn_require=start)
    def pos(self):
        return self.mt.start

    @row_annotation(fn_require=[contig, pos])
    def xpos(self):
        locus = hl.struct(contig=self.mt.contig, position=self.mt.pos),
        return variant_id.get_expr_for_xpos(locus)

    @row_annotation(fn_require=xpos)
    def xstart(self):
        return self.mt.xpos

    @row_annotation(fn_require=[contig, end])
    def xstop(self):
        locus = hl.struct(contig=self.mt.contig, position=self.mt.end),
        return variant_id.get_expr_for_xpos(locus)

class SeqrGCNVGenotypesSchema(SeqrGenotypesSchema):

    @row_annotation(fn_require=SeqrGenotypesSchema.genotypes)
    def samples(self):
        return self._genotype_filter_samples(lambda g: True)

    @row_annotation(fn_require=SeqrGenotypesSchema.genotypes)
    def samples_new_call(self):
        return self._genotype_filter_samples(lambda g: g.new_call)

    def samples_num_alt(self):
        pass

    def samples_gq(self):
        pass

    def samples_ab(self):
        pass

    @row_annotation(name="samples_qs", fn_require=SeqrGenotypesSchema.genotypes)
    def samples_qs(self, start=0, end=1000, step=10):
        return hl.struct(**{
            '%i_to_%i' % (i, i+step): self._genotype_filter_samples(lambda g: ((g.qs >= i) & (g.qs < i+step)))
            for i in range(start, end, step)
        }, **{
            "samples_qs_gt_1000": self._genotype_filter_samples(lambda g: g.gs > 1000)
        })

    @row_annotation(name="samples_cn", fn_require=SeqrGenotypesSchema.genotypes)
    def samples_cn(self, start=0, end=4, step=1):
        return hl.struct(**{
            '%i' % i: self._genotype_filter_samples(lambda g: g.cn == i)
            for i in range(start, end, step)
        }, **{
            "samples_cn_gte_4": self._genotype_filter_samples(lambda g: g.cn >= 4)
        })

    def _genotype_filter_samples(self, filter):
        # Overriden to support existing 
        samples = self.mt.genotypes.filter(filter).map(lambda g: g.sample_id)
        return hl.if_else(hl.len(samples) > 0, samples, hl.missing(hl.dtype('list<str>')))
    
    def _genotype_fields(self):
        return {
            'sample_id': get_seqr_sample_id(self.mt.sample_fix),
            'qs': self.mt.QS,
            'cn': self.mt.CN,
            'defragged': self.mt.defragmented,
            # Hail expression is to bool-ify a string value.
            'prev_call': hl.if_else(hl.len(table.identical_ovl) > 0, True, False) if self.is_new_joint_call else not self.mt.is_latest,
            'prev_overlap': hl.if_else(hl.len(table.any_ovl) > 0, True, False)  if self.is_new_joint_call else False,
            # NB: previous implementation also falsified NA, but hail treats NA as an empty value.
            'new_call': self.mt.no_ovl if self.is_new_joint_call else False,
        }

class SeqrGCNVVariantsAndGenotypesSchema(SeqrGCNVVariantSchema, SeqrGCNVGenotypesSchema):
    
    @staticmethod
    def elasticsearch_row(ds):
        return SeqrVariantsAndGenotypesSchema.elasticsearch_row(ds)
