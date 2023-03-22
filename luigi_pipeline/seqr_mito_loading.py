import logging
import sys

import luigi
import hail as hl

from luigi_pipeline.lib.model.mito_mt_schema import SeqrMitoVariantsAndGenotypesSchema, SeqrMitoVariantSchema, SeqrMitoGenotypesSchema
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
    VariantTask = SeqrMitoVariantMTTask
    GenotypesSchema = SeqrMitoGenotypesSchema

class SeqrMitoMTToESTask(BaseMTToESOptimizedTask):
    VariantTask = SeqrMitoVariantMTTask
    GenotypesTask = SeqrMitoGenotypesMTTask
    VariantsAndGenotypesSchema = SeqrMitoVariantsAndGenotypesSchema


if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)
