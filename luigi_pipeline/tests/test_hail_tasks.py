import shutil, tempfile, os, unittest

import hail as hl
import luigi

from lib.hail_tasks import HailMatrixTableTask, HailElasticSearchTask

TEST_DATA_MT_1KG = 'tests/data/1kg_30variants.vcf.bgz'

class TestHailTasks(unittest.TestCase):

    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        # Remove the directory after the test
        shutil.rmtree(self.test_dir)

    def test_hail_matrix_table_load(self):
        temp_dest_path = os.path.join(self.test_dir, 'temp_test_load.mt')
        task = HailMatrixTableTask(source_paths=[TEST_DATA_MT_1KG], dest_path=temp_dest_path)

        mt = task.import_vcf()
        self.assertTrue(mt.count() == (30, 16))

    def test_hail_matrix_table_run(self):
        temp_dest_path = os.path.join(self.test_dir, 'temp_test_run.mt')

        task = HailMatrixTableTask(source_paths=[TEST_DATA_MT_1KG], dest_path=temp_dest_path)
        task.run()

        mt = hl.read_matrix_table(temp_dest_path)
        self.assertTrue(mt.count() == (30, 16))

    def test_hail_matrix_table_and_elasticsearch_tasks(self):
        temp_dest_path = os.path.join(self.test_dir, 'temp_test_mt_and_es.mt')

        class ESTask(HailElasticSearchTask):
            def requires(self):
                return [
                    HailMatrixTableTask(source_paths=[TEST_DATA_MT_1KG],
                                        dest_path=temp_dest_path)
                ]

        es_task = ESTask()
        w = luigi.worker.Worker()
        w.add(es_task)
        w.run()

        mt = es_task.import_mt()
        self.assertTrue(mt.count() == (30, 16))
