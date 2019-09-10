"""
Tasks for Hail.
"""
import logging
import math
import os

import hail as hl
import luigi
from luigi.contrib import gcs

from hail_scripts.v02.utils.elasticsearch_client import ElasticsearchClient
from lib.global_config import GlobalConfig
import lib.hail_vep_runners as vep_runners

logger = logging.getLogger(__name__)


class MatrixTableSampleSetError(Exception):
    def __init__(self, message, missing_samples):
        super().__init__(message)
        self.missing_samples = missing_samples


def GCSorLocalTarget(filename):
    target = gcs.GCSTarget if filename.startswith('gs://') else luigi.LocalTarget
    return target(filename)


class VcfFile(luigi.Task):
    filename = luigi.Parameter()
    def output(self):
       return GCSorLocalTarget(self.filename)


class HailMatrixTableTask(luigi.Task):
    """
    Task that reads in list of VCFs and writes as a Matrix Table. To be overwritten
    to provide specific operations.
    Does not run if dest path exists (complete) or the source path does not (fail).
    """
    source_paths = luigi.ListParameter(description='List of paths to VCFs to be loaded.')
    dest_path = luigi.Parameter(description='Path to write the matrix table.')
    genome_version = luigi.Parameter(description='Reference Genome Version (37 or 38)')
    vep_runner = luigi.ChoiceParameter(choices=['VEP', 'DUMMY'], default='VEP', description='Choice of which vep runner'
                                                                                            'to annotate vep.')

    def requires(self):
        # We only exclude globs in source path here so luigi does not check if the file exists
        return [VcfFile(filename=s) for s in self.source_paths if '*' not in s]

    def output(self):
        return GCSorLocalTarget(self.dest_path)

    def complete(self):
        # Complete is called by Luigi to check if the task is done and will skip if it is.
        # By default it checks to see that the output exists, but we want to check for the
        # _SUCCESS file to make sure it was not terminated halfway.
        return GCSorLocalTarget(os.path.join(self.dest_path, '_SUCCESS')).exists()

    def run(self):
        # Overwrite to do custom transformations.
        mt = self.import_vcf()
        mt.write(self.output().path)

    def import_vcf(self):
        # Import the VCFs from inputs.
        return hl.import_vcf([vcf_file for vcf_file in self.source_paths],
                             reference_genome='GRCh' + self.genome_version,
                             force_bgz=True)

    @staticmethod
    def sample_type_stats(mt, genome_version, threshold=0.3):
        """
        Calculate stats for sample type by checking against a list of common coding and non-coding variants.
        If the match for each respective type is over the threshold, we return a match.

        :param mt: Matrix Table to check
        :param genome_version: reference genome version
        :param threshold: if the matched percentage is over this threshold, we classify as match
        :return: a dict of coding/non-coding to dict with 'matched_count', 'total_count' and 'match' boolean.
        """
        stats = {}
        types_to_ht_path = {
            'noncoding': GlobalConfig().param_kwargs['validation_%s_noncoding_ht' % genome_version],
            'coding': GlobalConfig().param_kwargs['validation_%s_coding_ht' % genome_version]
        }
        for sample_type, ht_path in types_to_ht_path.items():
            ht = hl.read_table(ht_path)
            stats[sample_type] = ht_stats = {
                'matched_count': mt.semi_join_rows(ht).count_rows(),
                'total_count': ht.count(),

            }
            ht_stats['match'] = (ht_stats['matched_count']/ht_stats['total_count']) >= threshold
        return stats

    def run_vep(mt, genome_version, runner='VEP'):
        runners = {
            'VEP': vep_runners.HailVEPRunner,
            'DUMMY': vep_runners.HailVEPDummyRunner
        }

        return runners[runner]().run(mt, genome_version)

    @staticmethod
    def subset_samples_and_variants(mt, subset_path):
        """
        Subset the MatrixTable to the provided list of samples and to variants present in those samples
        :param mt: MatrixTable from VCF
        :param subset_path: Path to a file with a single column 's'
        :return: MatrixTable subsetted to list of samples
        """
        subset_ht = hl.import_table(subset_path, key='s')
        subset_count = subset_ht.count()
        anti_join_ht = subset_ht.anti_join(mt.cols())
        anti_join_ht_count = anti_join_ht.count()

        if anti_join_ht_count != 0:
            missing_samples = anti_join_ht.s.collect()
            raise MatrixTableSampleSetError(
                f'Only {subset_count-anti_join_ht_count} out of {subset_count} '
                'subsetting-table IDs matched IDs in the variant callset.\n'
                f'IDs that aren\'t in the callset: {missing_samples}\n'
                f'All callset sample IDs:{mt.s.collect()}', missing_samples
            )

        mt = mt.semi_join_cols(subset_ht)
        mt = mt.filter_rows((hl.agg.count_where(mt.GT.is_non_ref())) > 0)

        logger.info(f'Finished subsetting samples. Kept {anti_join_ht_count} '
                    f'out of {mt.count()} samples in vds')
        return mt

    @staticmethod
    def remap_sample_ids(mt, remap_path):
        """
        Remap the MatrixTable's sample ID, 's', field to the sample ID used within seqr, 'seqr_id'
        If the sample 's' does not have a 'seqr_id' in the remap file, 's' becomes 'seqr_id'
        :param mt: MatrixTable from VCF
        :param remap_path: Path to a file with two columns 's' and 'seqr_id'
        :return: MatrixTable remapped and keyed to use seqr_id
        """
        remap_ht = hl.import_table(remap_path, key='s')
        missing_samples = remap_ht.anti_join(mt.cols()).collect()
        remap_count = remap_ht.count()

        if len(missing_samples) != 0:
            raise MatrixTableSampleSetError(
                f'Only {remap_ht.semi_join(mt.cols()).count()} out of {remap_count} '
                'remap IDs matched IDs in the variant callset.\n'
                f'IDs that aren\'t in the callset: {missing_samples}\n'
                f'All callset sample IDs:{mt.s.collect()}', missing_samples
            )

        mt = mt.annotate_cols(**remap_ht[mt.s])
        remap_expr = hl.cond(hl.is_missing(mt.seqr_id), mt.s, mt.seqr_id)
        mt = mt.annotate_cols(seqr_id=remap_expr, vcf_id=mt.s)
        mt = mt.key_cols_by(s=mt.seqr_id)
        logger.info(f'Remapped {remap_count} sample ids...')
        return mt


class HailElasticSearchTask(luigi.Task):
    """
    Loads a MT to ES (TODO).
    """
    source_path = luigi.OptionalParameter(default=None)
    use_temp_loading_nodes = luigi.BoolParameter(default=True, description='Whether to use temporary loading nodes.')
    es_host = luigi.Parameter(description='ElasticSearch host.', default='localhost')
    es_port = luigi.IntParameter(description='ElasticSearch port.', default=9200)
    es_index = luigi.Parameter(description='ElasticSearch index.', default='data')
    es_index_min_num_shards = luigi.IntParameter(default=6,
                                                 description='Number of shards for the index will be the greater of '
                                                             'this value and a calculated value based on the matrix.')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._es = ElasticsearchClient(host=self.es_host, port=self.es_port)

    def requires(self):
        return [VcfFile(filename=self.source_path)]

    def run(self):
        mt = self.import_mt()
        # TODO: Load into ES

    def import_mt(self):
        return hl.read_matrix_table(self.input()[0].path)

    def export_table_to_elasticsearch(self, table, num_shards):
        func_to_run_after_index_exists = None if not self.use_temp_loading_nodes else \
            lambda: self.route_index_to_temp_es_cluster(True)
        self._es.export_table_to_elasticsearch(table,
                                               index_name=self.es_index,
                                               func_to_run_after_index_exists=func_to_run_after_index_exists,
                                               elasticsearch_mapping_id="docId",
                                               num_shards=num_shards,
                                               write_null_values=True)

    def cleanup(self):
        self.route_index_to_temp_es_cluster(False)

    def route_index_to_temp_es_cluster(self, to_temp_loading):
        """Apply shard allocation filtering rules for the given index to elasticsearch data nodes with *loading* in
        their name:

        If to_temp_loading is True, route new documents in the given index only to nodes named "*loading*".
        Otherwise, move any shards in this index off of nodes named "*loading*"

        Args:
            to_temp_loading (bool): whether to route shards in the given index to the "*loading*" nodes, or move
            shards off of these nodes.
        """
        if to_temp_loading:
            require_name = "es-data-loading*"
            exclude_name = ""
        else:
            require_name = ""
            exclude_name = "es-data-loading*"

        body = {
            "index.routing.allocation.require._name": require_name,
            "index.routing.allocation.exclude._name": exclude_name
        }

        logger.info("==> Setting {}* settings = {}".format(self.es_index, body))

        index_arg = "{}*".format(self.es_index)
        self._es.es.indices.put_settings(index=index_arg, body=body)

    def _mt_num_shards(self, mt):
        # The greater of the user specified min shards and calculated based on the variants and samples
        denominator = 1.4*10**9
        calculated_num_shards = math.ceil((mt.count_rows() * mt.count_cols())/denominator)
        return max(self.es_index_min_num_shards, calculated_num_shards)
