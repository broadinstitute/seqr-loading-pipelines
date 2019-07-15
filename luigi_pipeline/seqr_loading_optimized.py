import logging
import os
import re

import luigi
import hail as hl

from hail_scripts.v02.utils.elasticsearch_client import ElasticsearchClient
from lib.hail_tasks import HailMatrixTableTask, HailElasticSearchTask, GCSorLocalTarget, MatrixTableSampleSetError
from lib.model.seqr_mt_schema import SeqrSchema, SeqrVariantSchema, SeqrGenotypesSchema
import seqr_loading
# from gnomad_hail.utils import vep_or_lookup_vep

logger = logging.getLogger(__name__)


class SeqrValidationError(Exception):
    pass

class SeqrVCFToVariantHTTask(seqr_loading.SeqrVCFToMTTask):
    """
    Loads all annotations for the variants of a VCF into a Hail Table (parent class of MT is a misnomer).
    """

    def run(self):
        mt = self.import_vcf()
        mt = hl.split_multi_hts(mt)
        if self.validate:
            self.validate_mt(mt, self.genome_version, self.sample_type)
        mt = HailMatrixTableTask.run_vep(mt, self.genome_version, self.vep_runner)
        # We're now adding ref data.
        ref_data = hl.read_table(self.reference_ht_path)
        clinvar = hl.read_table(self.clinvar_ht_path)
        hgmd = hl.read_table(self.hgmd_ht_path)

        ht = SeqrVariantSchema(mt, ref_data=ref_data, clinvar_data=clinvar, hgmd_data=hgmd).annotate_all(
            overwrite=True).select_annotated_mt().rows()

        ht.describe()

        ht.write(self.output().path, stage_locally=True)
        hl.copy_log(self.output().path + '-logs.log')

class SeqrVCFToGenotypesMTTask(HailMatrixTableTask):
    remap_path = luigi.OptionalParameter(default=None,
                                         description="Path to a tsv file with two columns: s and seqr_id.")
    subset_path = luigi.OptionalParameter(default=None,
                                          description="Path to a tsv file with one column of sample IDs: s.")
    def run(self):
        mt = self.import_vcf()
        mt = hl.split_multi_hts(mt)

        if self.remap_path:
            mt = self.remap_sample_ids(mt, self.remap_path)
        if self.subset_path:
            mt = self.subset_samples_and_variants(mt, self.subset_path)

        mt = SeqrGenotypesSchema(mt).annotate_all(overwrite=True).select_annotated_mt()

        mt.describe()
        mt.write(self.output().path)


class SeqrMTToESOptimizedTask(HailElasticSearchTask):

    def __init__(self, *args, **kwargs):
        # TODO: instead of hardcoded index, generate from project_guid, etc.
        super().__init__(*args, **kwargs)

    def requires(self):
        return [SeqrVCFToVariantHTTask(), SeqrVCFToGenotypesMTTask()]

    def run(self):
        genotypes_mt = hl.read_matrix_table(self.input()[1].path)
        variants_ht = hl.read_table(self.input()[0].path)
        row_ht = genotypes_mt.rows().join(variants_ht)

        row_ht = row_ht.drop('vep').flatten()
        row_ht = row_ht.drop(row_ht.locus, row_ht.alleles).annotate(type='optimized')
        self.export_table_to_elasticsearch(row_ht)

        self.cleanup()


if __name__ == '__main__':
    luigi.run()