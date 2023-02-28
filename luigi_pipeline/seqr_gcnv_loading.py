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

    is_new_joint_call = luigi.BoolParameter(default=False, description='Is this a fully joint-called callset.')

    def run(self):
        ht = hl.import_table(self.source_paths[0], impute=True, min_partitions=500)
        mt = ht.to_matrix_table(
            row_key=['variant_name', 'svtype'], col_key=['sample_fix'],
            # Analagous to CORE_COLUMNS = [CHR_COL, SC_COL, SF_COL, CALL_COL, IN_SILICO_COL] in the old implementation
            row_fields=['chr', 'vac', 'vaf', 'strvctvre_score'],
        )
        # This rename helps disambiguate between the 'start' & 'end' that are aggregations
        # over samples and the start and end of each sample.
        mt = mt.rename({'start': 'sample_start', 'end': 'sample_end'})

        if self.remap_path:
            mt = self.remap_sample_ids(mt, self.remap_path)
        if self.subset_path:
            mt = self.subset_samples_and_variants(mt, self.subset_path)
        
        kwargs = self.get_schema_class_kwargs()
        mt = self.SCHEMA_CLASS(mt, **kwargs).annotate_all(overwrite=True).select_annotated_mt()
        mt = self.annotate_globals(mt)
        
        mt.describe()
        mt.write(self.output().path, stage_locally=True, overwrite=True)       

    def get_schema_class_kwargs(self):
        return {
            "is_new_joint_call" : self.is_new_joint_call
        }


class SeqrGCNVGenotypesMTTask(BaseVCFToGenotypesMTTask):
    VariantTask = SeqrGCNVVariantMTTask
    GenotypesSchema = SeqrGCNVGenotypesSchema

class SeqrGCNVMTToESTask(BaseMTToESOptimizedTask):
    VariantTask = SeqrGCNVVariantMTTask
    GenotypesTask = SeqrGCNVGenotypesMTTask
    VariantsAndGenotypesSchema = SeqrGCNVVariantsAndGenotypesSchema


if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)
