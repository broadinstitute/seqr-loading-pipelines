import datetime
import re

import hail as hl

from hail_scripts.computed_fields import variant_id

from luigi_pipeline.lib.model.base_mt_schema import row_annotation
from luigi_pipeline.lib.model.seqr_mt_schema import (
    BaseVariantSchema,
    SeqrGenotypesSchema,
    SeqrVariantsAndGenotypesSchema,
)

GENE_ID = "gene_id"
MAJOR_CONSEQUENCE = "major_consequence"

def parse_genes(gene_col: hl.expr.StringExpression) -> hl.expr.SetExpression:
    """
    Convert a string-ified gene list to a set()
    """
    return hl.set(gene_col.split(',').filter(
        lambda gene: ~hl.set({'None', 'null', 'NA', ''}).contains(gene)
    ).map(
        lambda gene: gene.split(r'\.')[0]
    ))

def hl_agg_collect_set_union(gene_col: hl.expr.SetExpression) -> hl.expr.SetExpression:
    return hl.flatten(hl.agg.collect_as_set(gene_col))

class SeqrGCNVVariantSchema(BaseVariantSchema):

    @row_annotation(disable_index=True)
    def contig(self):
        return variant_id.replace_chr_prefix(self.mt.chr)

    @row_annotation()
    def sc(self):
        return self.mt.vac

    @row_annotation()
    def sf(self):
        return self.mt.vaf

    @row_annotation()
    def sn(self):
        return hl.or_missing(
            hl.is_defined(self.mt.vaf),
            hl.int(self.mt.vac / self.mt.vaf),
        )

    @row_annotation(name='svType')
    def sv_type(self):
        return self.mt.svtype

    @row_annotation(name='StrVCTVRE_score')
    def strvctvre(self):
       return self.mt.strvctvre_score

    @row_annotation(name='variantId', disable_index=True)
    def variant_id(self):
        return hl.format(f"%s_%s_{datetime.date.today():%m%d%Y}", self.mt.variant_name, self.mt.svtype)

    @row_annotation(disable_index=True)
    def start(self):
        return hl.agg.min(self.mt.sample_start)

    @row_annotation()
    def end(self):
        return hl.agg.max(self.mt.sample_end)

    @row_annotation()
    def num_exon(self):
        return hl.agg.max(self.mt.genes_any_overlap_totalExons)

    @row_annotation(name='geneIds')
    def gene_ids(self):
        return hl.array(hl_agg_collect_set_union(parse_genes(self.mt.genes_any_overlap_Ensemble_ID)))

    @row_annotation(name='sortedTranscriptConsequences', fn_require=gene_ids)
    def sorted_transcript_consequences(self):
        lof_genes = hl_agg_collect_set_union(parse_genes(self.mt.genes_LOF_Ensemble_ID))
        copy_gain_genes = hl_agg_collect_set_union(parse_genes(self.mt.genes_CG_Ensemble_ID))
        major_consequence_genes = lof_genes | copy_gain_genes
        return hl.map(
            lambda gene: hl.if_else(
                major_consequence_genes.contains(gene),
                {
                    GENE_ID: gene,
                    MAJOR_CONSEQUENCE: hl.if_else(
                        lof_genes.contains(gene),
                        "LOF",
                        "COPY_GAIN",
                    )
                },
                {GENE_ID: gene},
            ),
            self.mt.geneIds,
        )

    @row_annotation(name='transcriptConsequenceTerms', fn_require=[
        sv_type,
        sorted_transcript_consequences,
    ])
    def transcript_consequence_terms(self):
        default_consequences = [hl.format('gCNV_%s', self.mt.svType)]
        gene_major_consequences = hl.array(hl.set(
            self.mt.sortedTranscriptConsequences
            .filter(lambda x: x.contains(MAJOR_CONSEQUENCE))
            .map(lambda x: x[MAJOR_CONSEQUENCE])
        ))
        return gene_major_consequences.extend(default_consequences)

    @row_annotation(fn_require=start)
    def pos(self):
        return self.mt.start

    @row_annotation(fn_require=[contig, pos])
    def xpos(self):
        return variant_id.get_expr_for_xpos(
            hl.locus(self.mt.contig, self.mt.pos)
        )

    @row_annotation(disable_index=True, fn_require=xpos)
    def xstart(self):
        return self.mt.xpos

    @row_annotation(fn_require=[contig, end])
    def xstop(self):
        return variant_id.get_expr_for_xpos(
            hl.locus(self.mt.contig, self.mt.end)
        )

class SeqrGCNVGenotypesSchema(SeqrGenotypesSchema):

    def __init__(self, *args, is_new_joint_call=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._is_new_joint_call = is_new_joint_call

    @row_annotation(fn_require=SeqrGenotypesSchema.genotypes)
    def samples(self):
        return self._genotype_filter_samples(lambda g: True)

    @row_annotation(fn_require=SeqrGenotypesSchema.genotypes)
    def samples_new_call(self):
        return self._genotype_filter_samples(lambda g: g.new_call)

    def samples_no_call(self):
        pass

    def samples_num_alt(self):
        pass

    def samples_gq(self):
        pass

    def samples_ab(self):
        pass

    @row_annotation(fn_require=SeqrGenotypesSchema.genotypes)
    def samples_qs(self, start=0, end=1000, step=10):
        return hl.struct(**{
            f'{i}_to_{i + step}': self._genotype_filter_samples(lambda g: ((g.qs >= i) & (g.qs < i+step)))
            for i in range(start, end, step)
        }, **{
            "samples_qs_gt_1000": self._genotype_filter_samples(lambda g: g.qs >= 1000)
        })

    @row_annotation(name="samples_cn", fn_require=SeqrGenotypesSchema.genotypes)
    def samples_cn(self, start=0, end=4, step=1):
        return hl.struct(**{
            f'{i}': self._genotype_filter_samples(lambda g: g.cn == i)
            for i in range(start, end, step)
        }, **{
            "samples_cn_gte_4": self._genotype_filter_samples(lambda g: g.cn >= 4)
        })

    def _genotype_filter_samples(self, filter):
        samples = self.mt.genotypes.filter(filter).map(lambda g: g.sample_id)
        return hl.if_else(hl.len(samples) > 0, samples, hl.missing(hl.dtype('array<str>')))
    
    def _genotype_fields(self):
        if self._is_new_joint_call:
            call_fields = {
                'prev_call': (hl.len(self.mt.identical_ovl) > 0),
                'prev_overlap': (hl.len(self.mt.any_ovl) > 0),
                'new_call': self.mt.no_ovl,
            }
        else:
            call_fields = {
                'prev_call': ~self.mt.is_latest,
                'prev_overlap': False,
                'new_call': False,
            }

        parsed_genes = hl.array(parse_genes(self.mt.genes_any_overlap_Ensemble_ID))
        start_and_end_equal = (self.mt.sample_start == self.mt.start) & (self.mt.sample_end == self.mt.end)
        return {
            'sample_id': self.mt.s,
            'qs': self.mt.QS,
            'cn': self.mt.CN,
            'defragged': self.mt.defragmented,
            'start': hl.or_missing(~start_and_end_equal, self.mt.sample_start),
            'end': hl.or_missing(~start_and_end_equal, self.mt.sample_end),
            'num_exon': hl.or_missing(
                self.mt.genes_any_overlap_totalExons != self.mt.num_exon,
                self.mt.genes_any_overlap_totalExons,
            ),
            'geneIds': hl.or_missing(parsed_genes != self.mt.geneIds, parsed_genes),
            **call_fields,
        }

class SeqrGCNVVariantsAndGenotypesSchema(SeqrGCNVVariantSchema, SeqrGCNVGenotypesSchema):
    
    # NB: we override this method because the row keys are different.
    @staticmethod
    def elasticsearch_row(ds):
        return SeqrVariantsAndGenotypesSchema.elasticsearch_row(ds)
