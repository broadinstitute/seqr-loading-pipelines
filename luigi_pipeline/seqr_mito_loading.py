import logging
import sys

import luigi
import hail as hl

from lib.model.mito_mt_schema import SeqrMitoVariantsAndGenotypesSchema, SeqrMitoVariantSchema, SeqrMitoGenotypesSchema
from luigi_pipeline.seqr_loading_optimized import SeqrVCFToVariantMTTask, BaseVCFToGenotypesMTTask, BaseMTToESOptimizedTask
from luigi_pipeline.lib.hail_tasks import MatrixTableSampleSetError

logger = logging.getLogger(__name__)


class SeqrMitoVariantMTTask(SeqrVCFToVariantMTTask):
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
        return hl.read_matrix_table(self.source_paths[0])

    def annotate_globals(self, mt):
        # Remove all existing global fields and annotate a new 'datasetType' field
        mt = mt.select_globals(datasetType='MITO')

        return super().annotate_globals(mt)


class SeqrMitoGenotypesMTTask(BaseVCFToGenotypesMTTask):
    ignore_missing_samples = luigi.BoolParameter(default=False, description='Allow missing samples in the callset.')
    VariantTask = SeqrMitoVariantMTTask
    GenotypesSchema = SeqrMitoGenotypesSchema

    def subset_samples_and_variants(self, mt, subset_path):
        """
        Override the base class method to allow missing samples for mitochondria datasets
        """
        subset_ht = hl.import_table(subset_path, key='s')
        subset_count = subset_ht.count()
        anti_join_ht = subset_ht.anti_join(mt.cols())
        anti_join_ht_count = anti_join_ht.count()

        if anti_join_ht_count != 0:
            missing_samples = anti_join_ht.s.collect()
            message = f'Only {subset_count - anti_join_ht_count} out of {subset_count} ' \
                      f'subsetting-table IDs matched IDs in the variant callset.\n' \
                      f'IDs that aren\'t in the callset: {missing_samples}\n' \
                      f'All callset sample IDs:{mt.s.collect()}'
            if (subset_count > anti_join_ht_count) and self.ignore_missing_samples:
                logger.warning(message)
            else:
                raise MatrixTableSampleSetError(message, missing_samples)

        mt = mt.semi_join_cols(subset_ht)
        mt = mt.filter_rows(hl.agg.any(mt.GT.is_non_ref()))

        logger.info(f'Finished subsetting samples. Kept {subset_count} '
                    f'out of {mt.count()} samples in vds')
        return mt


class SeqrMitoMTToESTask(BaseMTToESOptimizedTask):
    VariantTask = SeqrMitoVariantMTTask
    GenotypesTask = SeqrMitoGenotypesMTTask
    VariantsAndGenotypesSchema = SeqrMitoVariantsAndGenotypesSchema


if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)
