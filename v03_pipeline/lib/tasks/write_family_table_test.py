import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.tasks.write_family_table import WriteFamilyTableTask

TEST_VCF = 'v03_pipeline/var/test/vcfs/1kg_30variants.vcf.bgz'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'


@patch('v03_pipeline.lib.paths.DataRoot')
class WriteFamilyTableTaskTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_local_datasets = tempfile.TemporaryDirectory().name

    def tearDown(self) -> None:
        if os.path.isdir(self._temp_local_datasets):
            shutil.rmtree(self._temp_local_datasets)

    def test_update_sample_lookup_table_task(self, mock_dataroot: Mock) -> None:
        mock_dataroot.LOCAL_DATASETS.value = self._temp_local_datasets
        worker = luigi.worker.Worker()

        wft_task = WriteFamilyTableTask(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            callset_path=TEST_VCF,
            project_guid='R0113_test_project',
            project_remap_path=TEST_REMAP,
            project_pedigree_path=TEST_PEDIGREE_3,
            family_id='abc',
        )
        worker.add(wft_task)
        worker.run()
        self.assertEqual(
            wft_task.output().path,
            f'{self._temp_local_datasets}/v03/GRCh38/SNV/families/abc/samples.ht',
        )
        self.assertTrue(wft_task.complete())
        ht = hl.read_table(wft_task.output().path)
        self.assertCountEqual(
            ht.globals.sample_ids.collect(),
            [
                ['HG00731_1', 'HG00732_1', 'HG00733_1'],
            ],
        )

        self.assertCountEqual(
            ht.entries.collect()[:5],
            [
                [
                    hl.Struct(gq=99, ab=0.0, dp=34),
                    hl.Struct(gq=99, ab=0.0, dp=34),
                    hl.Struct(gq=99, ab=0.0, dp=37),
                ],
                [
                    hl.Struct(gq=99, ab=0.0, dp=37),
                    hl.Struct(gq=66, ab=0.0, dp=24),
                    hl.Struct(gq=96, ab=0.0, dp=32),
                ],
                [
                    hl.Struct(gq=21, ab=1.0, dp=7),
                    hl.Struct(gq=24, ab=1.0, dp=8),
                    hl.Struct(gq=12, ab=1.0, dp=4),
                ],
                [
                    hl.Struct(gq=30, ab=0.3333333333333333, dp=3),
                    hl.Struct(gq=6, ab=0.0, dp=2),
                    hl.Struct(gq=61, ab=0.6, dp=5),
                ],
                [
                    hl.Struct(gq=99, ab=0.0, dp=35),
                    hl.Struct(gq=72, ab=0.0, dp=24),
                    hl.Struct(gq=93, ab=0.0, dp=31),
                ],
            ],
        )
