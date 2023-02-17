import logging
import sys

import luigi
import hail as hl

from luigi_pipeline.seqr_loading_optimized import SeqrVCFToVariantMTTask
from sv_pipeline.genome.utils.mapping_gene_ids import load_gencode


logger = logging.getLogger(__name__)


class SeqrSVVCFToMTTask(SeqrVCFToVariantMTTask):
    ignore_missing_samples = luigi.BoolParameter(default=False, description="Allow missing samples in the callset.")
    skip_sample_subset = luigi.BoolParameter(default=False, description=
        "Skip subsetting samples... only subset on present variants."
    )
    gencode_release = luigi.IntParameter(default=43)
    RUN_VEP = False

    # NB: electing not to override import_vcf here eventhough the inherited args are slightly different
    # than from the old pipeline.

    def get_schema_class_kwargs(self):
        return {
            "gene_id_mapping" : hl.literal(load_gencode(gencode_release))
        }


    def subset_samples_and_variants(self, *args):
        return super().subset_samples_and_variants(
            *args, 
            ignore_missing_samples=self.ignore_missing_samples,
            skip_sample_subset=self.skip_sample_subset,
        )




if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)