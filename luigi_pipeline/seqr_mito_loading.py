import logging
import sys

import hail as hl
import luigi

from luigi_pipeline.lib.model.mito_mt_schema import (
    SeqrMitoGenotypesSchema,
    SeqrMitoVariantsAndGenotypesSchema,
    SeqrMitoVariantSchema,
)
from luigi_pipeline.lib.hail_tasks import MatrixTableSampleSetError
from luigi_pipeline.seqr_loading_optimized import (
    BaseMTToESOptimizedTask,
    BaseVCFToGenotypesMTTask,
    SeqrVCFToVariantMTTask,
)

logger = logging.getLogger(__name__)


class SeqrMitoVariantMTTask(SeqrVCFToVariantMTTask):
    """
    Loads all annotations for the variants of a Matrix Table into a Hail Table.
    """
    dataset_type = 'MITO'
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
        mt = mt.select_globals(datasetType=self.dataset_type)

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
