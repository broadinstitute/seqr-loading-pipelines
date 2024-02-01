import shutil
import unittest
from unittest.mock import patch

import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.paths import relatedness_check_table_path, sex_check_table_path
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'
TEST_PEDIGREE_4 = 'v03_pipeline/var/test/pedigrees/test_pedigree_4.tsv'
TEST_SEX_CHECK_1 = 'v03_pipeline/var/test/sex_check/test_sex_check_1.ht'
TEST_RELATEDNESS_CHECK_1 = (
    'v03_pipeline/var/test/relatedness_check/test_relatedness_check_1.ht'
)


class WriteRemappedAndSubsettedCallsetTaskTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        shutil.copytree(
            TEST_SEX_CHECK_1,
            sex_check_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_VCF,
            ),
        )
        shutil.copytree(
            TEST_RELATEDNESS_CHECK_1,
            relatedness_check_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_VCF,
            ),
        )

    @patch('v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset.Env')
    def test_write_remapped_and_subsetted_callset_task(
        self,
        mock_env: unittest.mock.Mock,
    ) -> None:
        mock_env.CHECK_SEX_AND_RELATEDNESS = True
        worker = luigi.worker.Worker()
        wrsc_task = WriteRemappedAndSubsettedCallsetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_guid='R0113_test_project',
            project_remap_path=TEST_REMAP,
            project_pedigree_path=TEST_PEDIGREE_3,
            validate=False,
        )
        worker.add(wrsc_task)
        worker.run()
        self.assertTrue(wrsc_task.complete())
        mt = hl.read_matrix_table(wrsc_task.output().path)
        self.assertEqual(mt.count(), (30, 3))
        self.assertEqual(
            mt.globals.collect(),
            [
                hl.Struct(
                    families_failed_missing_samples={},
                    families_failed_relatedness_check={},
                    families_failed_sex_check={},
                    families={'abc_1': ['HG00731_1', 'HG00732_1', 'HG00733_1']},
                ),
            ],
        )

    @patch('v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset.Env')
    def test_write_remapped_and_subsetted_callset_task_failed_sex_check_family(
        self,
        mock_env: unittest.mock.Mock,
    ) -> None:
        mock_env.CHECK_SEX_AND_RELATEDNESS = True
        worker = luigi.worker.Worker()
        wrsc_task = WriteRemappedAndSubsettedCallsetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_guid='R0114_project4',
            project_remap_path=TEST_REMAP,
            project_pedigree_path=TEST_PEDIGREE_4,
            validate=False,
        )
        worker.add(wrsc_task)
        worker.run()
        self.assertTrue(wrsc_task.complete())
        mt = hl.read_matrix_table(wrsc_task.output().path)
        # NB: one "family"/"sample" has been removed because of a failed sex check!
        self.assertEqual(mt.count(), (30, 12))
