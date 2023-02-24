import logging
import sys

import luigi
import hail as hl

from lib.model.gcnv_mt_schema import SeqrGCNVVariantSchema, SeqrGCNVGenotypesSchema, SeqrGCNVVariantsAndGenotypesSchema
from luigi_pipeline.seqr_loading_optimized import SeqrVCFToVariantMTTask, BaseVCFToGenotypesMTTask, BaseMTToESOptimizedTask
from sv_pipeline.genome.utils.mapping_gene_ids import load_gencode


logger = logging.getLogger(__name__)


class SeqrGCNVVariantMTTask(SeqrVCFToVariantMTTask):
    # Overrided inherited required params.
    reference_ht_path = ""
    clinvar_ht_path = ""
    sample_type = "WES"
    genome_version = "38"
    dont_validate = True
    dataset_type = "SV"

    RUN_VEP = False
    SCHEMA_CLASS = SeqrgCNVVariantSchema

    is_new_joint_call = luigi.BoolParameter(default=False, description='Is this a fully joint-called callset.')

    def import_dataset(self):
        return hl.import_table(self.source_paths[0], impute=True)

    def get_schema_class_kwargs(self):
        return {
            "is_new_joint_call" : self.is_new_joint_call
        }


class SeqrGCNVGenotypesMTTask(BaseVCFToGenotypesMTTask):
    VariantTask = SeqrgCNVVariantMTTask
    GenotypesSchema = SeqrgCNVGenotypesSchema

class SeqrGCNVMTToESTask(BaseMTToESOptimizedTask):
    VariantTask = SeqrgCNVVariantMTTask
    GenotypesTask = SeqrgCNVGenotypesMTTask
    VariantsAndGenotypesSchema = SeqrgCNVVariantsAndGenotypesSchema


if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)
