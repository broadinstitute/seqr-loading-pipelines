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
    SCHEMA_CLASS = SeqrGCNVVariantSchema

    def annotate_old_and_split_multi_hts(self, mt, *args, **kwargs):
        return mt

    def add_37_coordinates(self, mt, *args, **kwargs):
        # TODO.. implement this (it was out of scope in the port but we want it eventually)
        return mt

    def generate_callstats(self, mt, *args, **kwargs):
        return mt

    def import_dataset(self):
        ht = hl.import_table(self.source_paths[0], impute=True, min_partitions=500)
        mt = ht.to_matrix_table(
            row_key=['variant_name', 'svtype'], col_key=['sample_fix'],
            # Analagous to CORE_COLUMNS = [CHR_COL, SC_COL, SF_COL, CALL_COL, IN_SILICO_COL] in the old implementation
            row_fields=['chr', 'vac', 'vaf', 'strvctvre_score'],
        )
        # This rename helps disambiguate between the 'start' & 'end' that are aggregations
        # over samples and the start and end of each sample.
        return mt.rename({'start': 'sample_start', 'end': 'sample_end'})

    def get_schema_class_kwargs(self):
        return {}     


class SeqrGCNVGenotypesMTTask(BaseVCFToGenotypesMTTask):
    VariantTask = SeqrGCNVVariantMTTask
    GenotypesSchema = SeqrGCNVGenotypesSchema

    is_new_joint_call = luigi.BoolParameter(default=False, description='Is this a fully joint-called callset.')

    def get_schema_class_kwargs(self):
        return {
            "is_new_joint_call" : self.is_new_joint_call
        }

class SeqrGCNVMTToESTask(BaseMTToESOptimizedTask):
    VariantTask = SeqrGCNVVariantMTTask
    GenotypesTask = SeqrGCNVGenotypesMTTask
    VariantsAndGenotypesSchema = SeqrGCNVVariantsAndGenotypesSchema


if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)
