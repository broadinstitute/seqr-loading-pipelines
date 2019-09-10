import logging
import os
import sys
import re

import luigi
import hail as hl

from lib.hail_tasks import HailMatrixTableTask, HailElasticSearchTask, GCSorLocalTarget, MatrixTableSampleSetError
from lib.model.seqr_mt_schema import SeqrSchema, SeqrVariantSchema, SeqrGenotypesSchema, SeqrVariantsAndGenotypesSchema
import seqr_loading

logger = logging.getLogger(__name__)


class SeqrVCFToVariantMTTask(seqr_loading.SeqrVCFToMTTask):
    """
    Loads all annotations for the variants of a VCF into a Hail Table (parent class of MT is a misnomer).
    """

    def run(self):
        # Hack that fixes something in Hail. TODO: Remove when Hail fix comes.
        hl._set_flags(newaggs=None)
        # We only want to use the Variant Schema.
        self.read_vcf_write_mt(schema_cls=SeqrVariantSchema)


class SeqrVCFToGenotypesMTTask(HailMatrixTableTask):
    remap_path = luigi.OptionalParameter(default=None,
                                         description="Path to a tsv file with two columns: s and seqr_id.")
    subset_path = luigi.OptionalParameter(default=None,
                                          description="Path to a tsv file with one column of sample IDs: s.")

    def requires(self):
        return [SeqrVCFToVariantMTTask()]

    def run(self):
        # Hack that fixes something in Hail. TODO: Remove when Hail fix comes.
        hl._set_flags(newaggs=None)
        mt = hl.read_matrix_table(self.input()[0].path)

        if self.remap_path:
            mt = self.remap_sample_ids(mt, self.remap_path)
        if self.subset_path:
            mt = self.subset_samples_and_variants(mt, self.subset_path)

        mt = SeqrGenotypesSchema(mt).annotate_all(overwrite=True).select_annotated_mt()

        mt.describe()
        mt.write(self.output().path, stage_locally=True, overwrite=True)


class SeqrMTToESOptimizedTask(HailElasticSearchTask):

    def __init__(self, *args, **kwargs):
        # TODO: instead of hardcoded index, generate from project_guid, etc.
        super().__init__(*args, **kwargs)

    def requires(self):
        return [SeqrVCFToVariantMTTask(), SeqrVCFToGenotypesMTTask()]

    def run(self):
        # Hack that fixes something in Hail. TODO: Remove when Hail fix comes.
        hl._set_flags(newaggs=None)
        variants_mt = hl.read_matrix_table(self.input()[0].path)
        genotypes_mt = hl.read_matrix_table(self.input()[1].path)
        row_ht = genotypes_mt.rows().join(variants_mt.rows())

        row_ht = SeqrVariantsAndGenotypesSchema.elasticsearch_row(row_ht)
        self.export_table_to_elasticsearch(row_ht, self._mt_num_shards(genotypes_mt))

        self.cleanup()


if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)
