import logging
import sys

import luigi
import hail as hl

from lib.model.gcnv_mt_schema import SeqrgCNVVariantSchema, SeqrgCNVGenotypesSchema, SeqrgCNVVariantsAndGenotypesSchema
from luigi_pipeline.seqr_loading_optimized import SeqrVCFToVariantMTTask, BaseVCFToGenotypesMTTask, BaseMTToESOptimizedTask
from sv_pipeline.genome.utils.mapping_gene_ids import load_gencode


logger = logging.getLogger(__name__)


class SeqrgCNVVariantMTTask(SeqrVCFToVariantMTTask):
    # Overrided inherited required params.
    reference_ht_path = ""
    clinvar_ht_path = ""
    sample_type = "WES"
    genome_version = "38"
    dont_validate = True
    dataset_type = "SV"

    RUN_VEP = False
    SCHEMA_CLASS = SeqrgCNVVariantSchema

    def import_dataset(self):
        return hl.import_table(self.source_paths[0], impute=True)


class SeqrgCNVGenotypesMTTask(BaseVCFToGenotypesMTTask):
    VariantTask = SeqrgCNVVariantMTTask
    GenotypesSchema = SeqrgCNVGenotypesSchema

class SeqrSVMTToESTask(BaseMTToESOptimizedTask):
    VariantTask = SeqrgCNVVariantMTTask
    GenotypesTask = SeqrgCNVGenotypesMTTask
    VariantsAndGenotypesSchema = SeqrgCNVVariantsAndGenotypesSchema


if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)
