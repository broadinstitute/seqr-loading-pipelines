import shutil, tempfile, os, unittest
from unittest.mock import patch, Mock

import hail as hl
import luigi

from elasticsearch.client.indices import IndicesClient
from lib.hail_tasks import HailMatrixTableTask, HailElasticSearchTask, MatrixTableSampleSetError
from lib.global_config import GlobalConfig

TEST_DATA_MT_1KG = 'tests/data/1kg_30variants.vcf.bgz'


class TestHailTasks(unittest.TestCase):

    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        # Remove the directory after the test
        shutil.rmtree(self.test_dir)

    def _temp_dest_path(self):
        return os.path.join(self.test_dir, 'temp_test_load.mt')

    def _hail_matrix_table_task(self):
        temp_dest_path = self._temp_dest_path()
        return HailMatrixTableTask(source_paths=[TEST_DATA_MT_1KG],
                                   dest_path=temp_dest_path,
                                   genome_version='37')

    def _set_validation_configs(self):
        global_config = GlobalConfig()
        global_config.param_kwargs['validation_37_coding_ht'] = global_config.validation_37_coding_ht = \
            'tests/data/validation_37_coding.ht'
        global_config.param_kwargs['validation_37_noncoding_ht'] = global_config.validation_37_noncoding_ht = \
            'tests/data/validation_37_noncoding.ht'

    def test_hail_matrix_table_load(self):
        task = self._hail_matrix_table_task()
        mt = task.import_vcf()
        self.assertEqual(mt.count(), (30, 16))

    def test_hail_matrix_table_run(self):
        task = self._hail_matrix_table_task()
        task.run()

        mt = hl.read_matrix_table(self._temp_dest_path())
        self.assertEqual(mt.count(), (30, 16))

    def test_mt_sample_type_stats_1kg_30(self):
        self._set_validation_configs()

        mt = hl.import_vcf(TEST_DATA_MT_1KG)
        stats = HailMatrixTableTask.sample_type_stats(mt, '37', 0.3)
        self.assertEqual(stats, {
            'noncoding': {'matched_count': 1, 'total_count': 2243, 'match': False},
            'coding': {'matched_count': 4, 'total_count': 359, 'match': False}
        })

    def test_mt_sample_type_stats_threshold(self):
        threshold = 0.5
        self._set_validation_configs()

        # Tested to get under threshold 0.5 of coding variants.
        coding_under_threshold_ht = hl.read_table(GlobalConfig().validation_37_coding_ht).sample(threshold-0.2, 0)
        # Tested to get over threshold 0.5 of non-coding variants.
        noncoding_over_threshold_ht = hl.read_table(GlobalConfig().validation_37_noncoding_ht).sample(threshold+0.2, 0)
        combined_mt = hl.MatrixTable.from_rows_table(
            coding_under_threshold_ht.union(noncoding_over_threshold_ht).distinct()
        )

        # stats should match with noncoding (over threshold) and not coding (under threshold)
        stats = HailMatrixTableTask.sample_type_stats(combined_mt, '37', threshold)
        self.assertEqual(stats, {
            'noncoding': {'matched_count': 1545, 'total_count': 2243, 'match': True},
            'coding': {'matched_count': 118, 'total_count': 359, 'match': False}
        })

    def test_hail_matrix_table_and_elasticsearch_tasks(self):
        mt_task = self._hail_matrix_table_task()
        class ESTask(HailElasticSearchTask):

            def __init__(self):
                super().__init__(es_index='test')

            def requires(self):
                return [
                    mt_task
                ]

        es_task = ESTask()
        w = luigi.worker.Worker()
        w.add(es_task)
        w.run()

        mt = es_task.import_mt()
        self.assertEqual(mt.count(), (30, 16))

    def _create_temp_sample_remap_file(self, mt, number_of_remaps):
        # Creates a file with two columns, s and remap_id, where remap_id is s + _1
        temp_input_file = os.path.join(self.test_dir, 'temp_remap_samples.txt')
        remap = mt.cols().head(number_of_remaps).key_by('s')
        remap = remap.annotate(seqr_id=remap.s + '_1')
        remap.export(temp_input_file, header=True)
        return temp_input_file

    def _create_temp_sample_subset_file(self, mt, number_of_subset_samples, fail=False):
        # Creates subset file using mt.s. If fail is set to true, creates a sample ID not present in the test mt
        temp_input_file = os.path.join(self.test_dir, 'temp_subset_samples.txt')
        subset = mt.cols().head(number_of_subset_samples)
        if fail:
            subset = subset.annotate(test='wrong_sample').key_by('test')
            subset = subset.drop('s').rename({'test': 's'})
        subset.export(temp_input_file, header=True)
        return temp_input_file

    def test_hail_matrix_table_remap_0(self):
        # Tests the remap_sample_id function when there are no samples to be remapped
        mt = hl.import_vcf(TEST_DATA_MT_1KG)
        remap_mt =  HailMatrixTableTask.remap_sample_ids(mt, self._create_temp_sample_remap_file(mt, 0))
        self.assertEqual(remap_mt.anti_join_cols(mt.cols()).count_cols(), 0)

    def test_hail_matrix_table_remap_1(self):
        # Tests the remap_sample_id function when a single sample needs to be remapped
        mt = hl.import_vcf(TEST_DATA_MT_1KG)
        remap_mt = HailMatrixTableTask.remap_sample_ids(mt, self._create_temp_sample_remap_file(mt, 1))
        self.assertEqual(remap_mt.anti_join_cols(mt.cols()).count_cols(), 1)

    def test_hail_matrix_table_subset_14(self):
        # Tests the subset_samples_and_variants function using 14 samples which should leave 29 variants
        mt = hl.import_vcf(TEST_DATA_MT_1KG)
        subset_mt = HailMatrixTableTask.subset_samples_and_variants(mt, self._create_temp_sample_subset_file(mt, 14))
        self.assertEqual(subset_mt.count(), (29,14))

    def test_hail_matrix_table_subset_raise_e(self):
        # Tests if subsetting with an incorrect sample ID will raise the MatrixTableSampleSetError
        mt = hl.import_vcf(TEST_DATA_MT_1KG)
        self.assertRaises(MatrixTableSampleSetError, HailMatrixTableTask.subset_samples_and_variants,
                          mt, self._create_temp_sample_subset_file(mt, 1, True))

    def test_hail_matrix_table_subset_wrong_sample_id_correct(self):
        # Tests if subsetting with an incorrect sample ID will raise the MatrixTableSampleSetError and return the appropriate wrong id
        mt = hl.import_vcf(TEST_DATA_MT_1KG)
        with self.assertRaises(MatrixTableSampleSetError) as e:
            HailMatrixTableTask.subset_samples_and_variants(mt, self._create_temp_sample_subset_file(mt, 1, True))
            self.assertEqual(e.missing_samples, ['wrong_sample'])

    @patch.object(IndicesClient, 'put_settings',)
    def test_route_index_to_temp_es_cluster_true(self, mock_es_client_class):
        index = 'idx'
        task = HailElasticSearchTask(es_index=index)
        task.route_index_to_temp_es_cluster(True)
        self.assertEqual(mock_es_client_class.call_args[1]['index'], index + '*')
        self.assertEqual(mock_es_client_class.call_args[1]['body'], {
            'index.routing.allocation.require._name': 'es-data-loading*', 'index.routing.allocation.exclude._name': ''})

    @patch.object(IndicesClient, 'put_settings', )
    def test_route_index_to_temp_es_cluster_false(self, mock_es_client_class):
        index = 'idx'
        task = HailElasticSearchTask(es_index=index)
        task.route_index_to_temp_es_cluster(False)
        self.assertEqual(mock_es_client_class.call_args[1]['index'], index + '*')
        self.assertEqual(mock_es_client_class.call_args[1]['body'], {
            'index.routing.allocation.require._name': '', 'index.routing.allocation.exclude._name': 'es-data-loading*'})
