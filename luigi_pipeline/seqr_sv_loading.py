import logging
import sys

import luigi
import hail as hl

from luigi_pipeline.lib.model.sv_mt_schema import SeqrSVVariantSchema, SeqrSVGenotypesSchema, SeqrSVVariantsAndGenotypesSchema
from luigi_pipeline.seqr_loading_optimized import SeqrVCFToVariantMTTask, BaseVCFToGenotypesMTTask, BaseMTToESOptimizedTask
from sv_pipeline.genome.utils.mapping_gene_ids import load_gencode


logger = logging.getLogger(__name__)


class SeqrSVVariantMTTask(SeqrVCFToVariantMTTask):
    # Overrided inherited required params.
    reference_ht_path = ""
    clinvar_ht_path = ""
    sample_type = "WGS"
    genome_version = "38"
    dont_validate = True
    dataset_type = "SV"

    gencode_release = luigi.IntParameter(default=42)
    gencode_path = luigi.OptionalParameter(default="", description="Path for downloaded gencode data")
    RUN_VEP = False
    SCHEMA_CLASS = SeqrSVVariantSchema

    # NB: electing not to override import_vcf here eventhough the inherited args are slightly different
    # than from the old pipeline.

    def get_schema_class_kwargs(self):
        return {
            "gene_id_mapping" : hl.literal(load_gencode(self.gencode_release, self.gencode_path))
        }

class SeqrSVGenotypesMTTask(BaseVCFToGenotypesMTTask):
    VariantTask = SeqrSVVariantMTTask
    GenotypesSchema = SeqrSVGenotypesSchema

class SeqrSVMTToESTask(BaseMTToESOptimizedTask):
    VariantTask = SeqrSVVariantMTTask
    GenotypesTask = SeqrSVGenotypesMTTask
    VariantsAndGenotypesSchema = SeqrSVVariantsAndGenotypesSchema


if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)
