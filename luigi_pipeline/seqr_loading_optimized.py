import logging
import sys

import luigi
import hail as hl

from collections import defaultdict

from lib.hail_tasks import HailMatrixTableTask, HailElasticSearchTask
from lib.model.seqr_mt_schema import SeqrVariantSchema, SeqrGenotypesSchema, SeqrVariantsAndGenotypesSchema
from seqr_loading import SeqrVCFToMTTask

logger = logging.getLogger(__name__)


class SeqrVCFToVariantMTTask(SeqrVCFToMTTask):
    """
    Loads all annotations for the variants of a VCF into a Hail Table (parent class of MT is a misnomer).
    """
    SCHEMA_CLASS = SeqrVariantSchema

    def run(self):
        # We only want to use the Variant Schema.
        self.read_input_write_mt()


class BaseVCFToGenotypesMTTask(HailMatrixTableTask):
    remap_path = luigi.OptionalParameter(default=None,
                                         description="Path to a tsv file with two columns: s and seqr_id.")
    subset_path = luigi.OptionalParameter(default=None,
                                          description="Path to a tsv file with one column of sample IDs: s.")

    def get_schema_class_kwargs(self):
        return {}

    def requires(self):
        return [self.VariantTask()]

    def run(self):
        mt = hl.read_matrix_table(self.input()[0].path)

        if self.remap_path:
            mt = self.remap_sample_ids(mt, self.remap_path)
        if self.subset_path:
            mt = self.subset_samples_and_variants(mt, self.subset_path)

        kwargs = self.get_schema_class_kwargs()
        mt = self.GenotypesSchema(mt, **kwargs).annotate_all(overwrite=True).select_annotated_mt()

        mt.describe()
        mt.write(self.output().path, stage_locally=True, overwrite=True)


class SeqrVCFToGenotypesMTTask(BaseVCFToGenotypesMTTask):
    VariantTask = SeqrVCFToVariantMTTask
    GenotypesSchema = SeqrGenotypesSchema


class BaseMTToESOptimizedTask(HailElasticSearchTask):

    def __init__(self, *args, **kwargs):
        # TODO: instead of hardcoded index, generate from project_guid, etc.
        super().__init__(*args, **kwargs)

    def requires(self):
        return [self.VariantTask(), self.GenotypesTask()]

    def run(self):
        variants_mt = hl.read_matrix_table(self.input()[0].path)
        genotypes_mt = hl.read_matrix_table(self.input()[1].path)
        genotypes_mt = genotypes_mt.drop(*[k for k in genotypes_mt.globals.keys()])
        row_ht = genotypes_mt.rows().join(variants_mt.rows())

        row_ht = self.VariantsAndGenotypesSchema.elasticsearch_row(row_ht)
        es_shards = self._mt_num_shards(genotypes_mt)

        # Initialize an empty SeqrVariantsAndGenotypesSchema to access class properties
        disabled_fields = self.VariantsAndGenotypesSchema(None, ref_data=defaultdict(dict), interval_ref_data=None, clinvar_data=None).get_disable_index_field()

        self.export_table_to_elasticsearch(table=row_ht, num_shards=es_shards, disabled_fields=disabled_fields)
        
        self.cleanup(es_shards)


class SeqrMTToESOptimizedTask(BaseMTToESOptimizedTask):
    VariantTask = SeqrVCFToVariantMTTask
    GenotypesTask = SeqrVCFToGenotypesMTTask
    VariantsAndGenotypesSchema = SeqrVariantsAndGenotypesSchema


if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)
