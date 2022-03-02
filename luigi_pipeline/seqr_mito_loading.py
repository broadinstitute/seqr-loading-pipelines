import logging
import sys

import luigi
import hail as hl

from lib.model.mito_mt_schema import SeqrMitoVariantsAndGenotypesSchema, SeqrMitoVariantSchema, SeqrMitoGenotypesSchema
from luigi_pipeline.seqr_loading_optimized import BaseVCFToGenotypesMTTask, BaseMTToESOptimizedTask
from seqr_loading import SeqrVCFToMTTask

logger = logging.getLogger(__name__)


class SeqrMitoVariantMTTask(SeqrVCFToMTTask):
    """
    Loads all annotations for the variants of a Matrix Table into a Hail Table.
    """
    high_constraint_interval_path = luigi.Parameter(description='Path to the tsv file storing the high constraint intervals.')
    RUN_VEP = False
    SCHEMA_CLASS = SeqrMitoVariantSchema

    def get_schema_class_kwargs(self):
        kwargs = super().get_schema_class_kwargs()
        kwargs['high_constraint_region'] = hl.import_locus_intervals(self.high_constraint_interval_path,
                                                                     reference_genome='GRCh38')
        return kwargs

    def import_dataset(self):
        mt = hl.read_matrix_table(self.source_paths[0])
        return mt.drop('dbsnp_version',
            'dp_hist_all_variants_n_larger', 'mq_hist_all_variants_bin_edges', 'dp_hist_all_variants_n_larger',
            'age_hist_all_samples_n_smaller', 'age_hist_all_samples_bin_freq', 'age_hist_all_samples_n_larger',
            'tlod_hist_all_variants_bin_freq', 'dp_hist_all_variants_bin_freq', 'mq_hist_all_variants_bin_freq',
            'col_annotation_descriptions', 'dp_hist_all_variants_bin_edges', 'mq_hist_all_variants_n_larger',
            'age_hist_all_samples_bin_edges', 'tlod_hist_all_variants_bin_edges', 'pop_order', 'hap_order',
            'global_annotation_descriptions', 'row_annotation_descriptions', 'tlod_hist_all_variants_n_larger')


class SeqrMitoGenotypesMTTask(BaseVCFToGenotypesMTTask):
    VariantTask = SeqrMitoVariantMTTask
    GenotypesSchema = SeqrMitoGenotypesSchema


class SeqrMitoMTToESTask(BaseMTToESOptimizedTask):
    VariantTask = SeqrMitoVariantMTTask
    GenotypesTask = SeqrMitoGenotypesMTTask
    VariantsAndGenotypesSchema = SeqrMitoVariantsAndGenotypesSchema


if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)
