import logging
import sys
import pkg_resources
import pprint

import luigi
import hail as hl

from lib.hail_tasks import HailElasticSearchTask
from lib.model.mito_mt_schema import SeqrMitoVariantsAndGenotypesSchema
from sv_pipeline.utils.common import get_es_index_name
import seqr_loading

logger = logging.getLogger(__name__)


class SeqrMitoMTTask(seqr_loading.SeqrVCFToMTTask):
    high_constraint_interval_path = luigi.Parameter(description='Path to the tsv file storing the high constraint intervals.')

    def read_annotate_write_mt(self, schema_cls=SeqrMitoVariantsAndGenotypesSchema):
        logger.info("Args:")
        pprint.pprint(self.__dict__)

        mt = hl.read_matrix_table(self.source_paths[0])

        #Drop unwanted global fields
        mt = mt.drop('dbsnp_version',
            'dp_hist_all_variants_n_larger', 'mq_hist_all_variants_bin_edges', 'dp_hist_all_variants_n_larger',
            'age_hist_all_samples_n_smaller', 'age_hist_all_samples_bin_freq', 'age_hist_all_samples_n_larger',
            'tlod_hist_all_variants_bin_freq', 'dp_hist_all_variants_bin_freq', 'mq_hist_all_variants_bin_freq',
            'col_annotation_descriptions', 'dp_hist_all_variants_bin_edges', 'mq_hist_all_variants_n_larger',
            'age_hist_all_samples_bin_edges', 'tlod_hist_all_variants_bin_edges', 'pop_order', 'hap_order',
            'global_annotation_descriptions', 'row_annotation_descriptions', 'tlod_hist_all_variants_n_larger')

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
        self.read_annotate_write_mt()


class SeqrMitoMTToESTask(HailElasticSearchTask):
    project_guid = luigi.Parameter(description='Project GUID.', default=None)

    def requires(self):
        return [SeqrMitoMTTask()]

    def run(self):
        mt = hl.read_matrix_table(self.input()[0].path)

        row_ht = SeqrMitoVariantsAndGenotypesSchema.elasticsearch_row(mt.rows())
        if self.project_guid:
            self.es_index = get_es_index_name(f'{self.project_guid}__mito__', mt.globals_table().take(1)[0])
        es_shards = self._mt_num_shards(mt)
        self.export_table_to_elasticsearch(row_ht, es_shards)

        self.cleanup(es_shards)


if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)
