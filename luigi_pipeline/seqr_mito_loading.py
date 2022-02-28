import logging
import sys
import pkg_resources
import pprint

import luigi
import hail as hl

from lib.model.mito_mt_schema import SeqrMitoVariantsAndGenotypesSchema, SeqrMitoVariantSchema, SeqrMitoGenotypesSchema
from luigi_pipeline.seqr_loading_optimized import BaseVCFToGenotypesMTTask, BaseMTToESOptimizedTask
import seqr_loading

logger = logging.getLogger(__name__)


class SeqrMitoVariantMTTask(seqr_loading.SeqrVCFToMTTask):
    """
    Loads all annotations for the variants of a VCF (MT) into a Hail Table (parent class of MT is a misnomer).
    """
    high_constraint_interval_path = luigi.Parameter(description='Path to the tsv file storing the high constraint intervals.')

    def read_mt_write_mt(self, schema_cls=SeqrMitoVariantsAndGenotypesSchema):
        logger.info("Args:")
        pprint.pprint(self.__dict__)

        mt = hl.read_matrix_table(self.source_paths[0])
        mt = mt.drop('dbsnp_version', 'vep_version',
            'dp_hist_all_variants_n_larger', 'mq_hist_all_variants_bin_edges', 'dp_hist_all_variants_n_larger',
            'age_hist_all_samples_n_smaller', 'age_hist_all_samples_bin_freq', 'age_hist_all_samples_n_larger',
            'tlod_hist_all_variants_bin_freq', 'dp_hist_all_variants_bin_freq', 'mq_hist_all_variants_bin_freq',
            'col_annotation_descriptions', 'dp_hist_all_variants_bin_edges', 'mq_hist_all_variants_n_larger',
            'age_hist_all_samples_bin_edges', 'tlod_hist_all_variants_bin_edges', 'pop_order', 'hap_order',
            'global_annotation_descriptions', 'row_annotation_descriptions', 'tlod_hist_all_variants_n_larger')
        if not self.dont_validate:
            self.validate_mt(mt, self.genome_version, self.sample_type)
        if self.remap_path:
            mt = self.remap_sample_ids(mt, self.remap_path)
        if self.subset_path:
            mt = self.subset_samples_and_variants(mt, self.subset_path)

        ref_data = hl.read_table(self.reference_ht_path)

        high_constraint_region = hl.import_locus_intervals(self.high_constraint_interval_path, reference_genome='GRCh38')

        mt = schema_cls(mt, ref_data=ref_data, high_constraint_region=high_constraint_region).annotate_all(
            overwrite=True).select_annotated_mt()

        mt = mt.annotate_globals(sourceFilePath=','.join(self.source_paths),
                                 genomeVersion=self.genome_version,
                                 sampleType=self.sample_type,
                                 hail_version=pkg_resources.get_distribution('hail').version)

        mt.describe()
        mt.write(self.output().path, stage_locally=True, overwrite=True)

    def run(self):
        # We only want to use the Variant Schema.
        self.read_mt_write_mt(schema_cls=SeqrMitoVariantSchema)


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
