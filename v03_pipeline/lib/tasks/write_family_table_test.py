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
            family_guid='abc',
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
                    hl.Struct(
                        GQ=99,
                        AB=0.0,
                        DP=34,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                    hl.Struct(
                        GQ=99,
                        AB=0.0,
                        DP=34,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                    hl.Struct(
                        GQ=99,
                        AB=0.0,
                        DP=37,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                ],
                [
                    hl.Struct(
                        GQ=99,
                        AB=0.0,
                        DP=37,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                    hl.Struct(
                        GQ=66,
                        AB=0.0,
                        DP=24,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                    hl.Struct(
                        GQ=96,
                        AB=0.0,
                        DP=32,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                ],
                [
                    hl.Struct(
                        GQ=21,
                        AB=1.0,
                        DP=7,
                        GT=hl.Call(alleles=[1, 1], phased=False),
                    ),
                    hl.Struct(
                        GQ=24,
                        AB=1.0,
                        DP=8,
                        GT=hl.Call(alleles=[1, 1], phased=False),
                    ),
                    hl.Struct(
                        GQ=12,
                        AB=1.0,
                        DP=4,
                        GT=hl.Call(alleles=[1, 1], phased=False),
                    ),
                ],
                [
                    hl.Struct(
                        GQ=30,
                        AB=0.3333333333333333,
                        DP=3,
                        GT=hl.Call(alleles=[0, 1], phased=False),
                    ),
                    hl.Struct(
                        GQ=6,
                        AB=0.0,
                        DP=2,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                    hl.Struct(
                        GQ=61,
                        AB=0.6,
                        DP=5,
                        GT=hl.Call(alleles=[0, 1], phased=False),
                    ),
                ],
                [
                    hl.Struct(
                        GQ=99,
                        AB=0.0,
                        DP=35,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                    hl.Struct(
                        GQ=72,
                        AB=0.0,
                        DP=24,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                    hl.Struct(
                        GQ=93,
                        AB=0.0,
                        DP=31,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                ],
            ],
        )
