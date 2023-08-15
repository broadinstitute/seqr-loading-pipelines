import logging
import sys

import hail as hl
import luigi

from luigi_pipeline.lib.model.gcnv_mt_schema import (
    SeqrGCNVGenotypesSchema,
    SeqrGCNVVariantsAndGenotypesSchema,
    SeqrGCNVVariantSchema,
)
from luigi_pipeline.seqr_loading_optimized import (
    BaseMTToESOptimizedTask,
    BaseVCFToGenotypesMTTask,
    SeqrVCFToVariantMTTask,
)

logger = logging.getLogger(__name__)

FIELD_TYPES = {
    "start": hl.tint32, 
    "end": hl.tint32, 
    "CN": hl.tint32,
    "QS": hl.tint32, 
    "defragmented": hl.tbool, 
    "sf": hl.tfloat64, 
    "sc": hl.tint32,
    "genes_any_overlap_totalExons": hl.tint32,
    "genes_strict_overlap_totalExons": hl.tint32,
    "no_ovl": hl.tbool, 
    "is_latest": hl.tbool
}

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

    def split_multi_hts(self, mt, *args, **kwargs):
        return mt

    def add_37_coordinates(self, mt, *args, **kwargs):
        # TODO.. implement this (it was out of scope in the port but we want it eventually)
        return mt

    def generate_callstats(self, mt, *args, **kwargs):
        return mt

    def import_dataset(self):
        ht = hl.import_table(self.source_paths[0], types=FIELD_TYPES, min_partitions=500)
        mt = ht.to_matrix_table(
            row_key=['variant_name', 'svtype'], col_key=['sample_fix'],
            # Analagous to CORE_COLUMNS = [CHR_COL, SC_COL, SF_COL, CALL_COL, IN_SILICO_COL] in the old implementation
            row_fields=['chr', 'sc', 'sf', 'strvctvre_score'],
        )

        # rename the sample id column before the sample subset happens
        mt = mt.key_cols_by(s = mt.sample_fix)

        # This rename helps disambiguate between the 'start' & 'end' that are aggregations
        # over samples and the start and end of each sample.
        return mt.rename({'start': 'sample_start', 'end': 'sample_end'})

    def get_schema_class_kwargs(self):
        return {}     


class SeqrGCNVGenotypesMTTask(BaseVCFToGenotypesMTTask):
    VariantTask = SeqrGCNVVariantMTTask
    GenotypesSchema = SeqrGCNVGenotypesSchema

    is_new_joint_call = luigi.BoolParameter(default=False, description='Is this a fully joint-called callset.')

    def relevant_variant_filter_fn(self, mt):
        return hl.is_defined(mt.GT)

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
