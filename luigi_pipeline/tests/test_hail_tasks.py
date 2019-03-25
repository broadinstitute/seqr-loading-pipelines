import shutil, tempfile, os, unittest

import hail as hl
import luigi

from lib.hail_tasks import HailMatrixTableTask, HailElasticSearchTask
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

    def test_mt_impute_sample_type_1kg_30(self):
        self._set_validation_configs()

        mt = hl.import_vcf(TEST_DATA_MT_1KG)
        stats = HailMatrixTableTask.hl_mt_impute_sample_type(mt, '37', 0.3)
        self.assertEqual(stats, {
            'noncoding': {'matched_count': 1, 'total_count': 2243, 'match': False},
            'coding': {'matched_count': 4, 'total_count': 359, 'match': False}
        })

    def test_mt_impute_sample_type_threshold(self):
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
        stats = HailMatrixTableTask.hl_mt_impute_sample_type(combined_mt, '37', threshold)
        self.assertEqual(stats, {
            'noncoding': {'matched_count': 1545, 'total_count': 2243, 'match': True},
            'coding': {'matched_count': 118, 'total_count': 359, 'match': False}
        })

    def test_hail_matrix_table_and_elasticsearch_tasks(self):
        mt_task = self._hail_matrix_table_task()
        class ESTask(HailElasticSearchTask):
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
