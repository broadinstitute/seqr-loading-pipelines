import json
import shutil
from unittest.mock import Mock, patch

import hail as hl
import luigi.worker

from v03_pipeline.lib.core import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.misc.io import remap_pedigree_hash
from v03_pipeline.lib.paths import (
    relatedness_check_table_path,
    sex_check_table_path,
)
from v03_pipeline.lib.tasks.write_pipeline_errors_for_run import (
    WriteValidationErrorsForRunTask,
)
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)
from v03_pipeline.lib.test.misc import copy_project_pedigree_to_mocked_dir
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_PEDIGREE_3_REMAP = 'v03_pipeline/var/test/pedigrees/test_pedigree_3_remap.tsv'
TEST_PEDIGREE_4_REMAP = 'v03_pipeline/var/test/pedigrees/test_pedigree_4_remap.tsv'
TEST_PEDIGREE_7 = 'v03_pipeline/var/test/pedigrees/test_pedigree_7.tsv'
TEST_SEX_CHECK_1 = 'v03_pipeline/var/test/sex_check/test_sex_check_1.ht'
TEST_RELATEDNESS_CHECK_1 = (
    'v03_pipeline/var/test/relatedness_check/test_relatedness_check_1.ht'
)

TEST_RUN_ID = 'manual__2024-04-03'


class WriteRemappedAndSubsettedCallsetTaskTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        # +-------------+---------------+
        # | s           | predicted_sex |
        # +-------------+---------------+
        # | str         | str           |
        # +-------------+---------------+
        # | "HG00731_1" | "F"           |
        # | "HG00732_1" | "M"           |
        # | "HG00733_1" | "F"           |
        # | "NA19675_1" | "F"           |
        # | "NA19678_1" | "M"           |
        # | "NA19679_1" | "F"           |
        # | "NA20870_1" | "F"           |
        # | "NA20872_1" | "M"           |
        # | "NA20874_1" | "F"           |
        # | "NA20875_1" | "F"           |
        # | "NA20876_1" | "F"           |
        # | "NA20877_1" | "F"           |
        # | "NA20878_1" | "M"           |
        # | "NA20881_1" | "M"           |
        # | "NA20885_1" | "F"           |
        # | "NA20888_1" | "F"           |
        # +-------------+---------------+
        shutil.copytree(
            TEST_SEX_CHECK_1,
            sex_check_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_VCF,
            ),
        )
        # +-------------+-------------+-------+-------+-------+----------+
        # | i           | j           |  ibd0 |  ibd1 |  ibd2 |   pi_hat |
        # +-------------+-------------+-------+-------+-------+----------+
        # | str         | str         | int32 | int32 | int32 |  float64 |
        # +-------------+-------------+-------+-------+-------+----------+
        # | "HG00731_1" | "HG00733_1" |     0 |     1 |     0 | 5.00e-01 |
        # | "HG00732_1" | "HG00733_1" |     0 |     1 |     0 | 5.00e-01 |
        # +-------------+-------------+-------+-------+-------+----------+
        shutil.copytree(
            TEST_RELATEDNESS_CHECK_1,
            relatedness_check_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_VCF,
            ),
        )

    def test_write_remapped_and_subsetted_callset_task(
        self,
    ) -> None:
        copy_project_pedigree_to_mocked_dir(
            TEST_PEDIGREE_3_REMAP,
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            SampleType.WGS,
            'R0113_test_project',
        )
        worker = luigi.worker.Worker()
        wrsc_task = WriteRemappedAndSubsettedCallsetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id=TEST_RUN_ID,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_guids=['R0113_test_project'],
            project_i=0,
            skip_validation=True,
            skip_expect_tdr_metrics=True,
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
                    remap_pedigree_hash=hl.eval(
                        remap_pedigree_hash(
                            TEST_PEDIGREE_3_REMAP,
                        ),
                    ),
                    failed_family_samples=hl.Struct(
                        missing_samples={},
                        relatedness_check={},
                        sex_check={},
                        ploidy_check={},
                    ),
                    family_samples={'abc_1': ['HG00731_1', 'HG00732_1', 'HG00733_1']},
                ),
            ],
        )

    @patch('v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset.FeatureFlag')
    def test_write_remapped_and_subsetted_callset_task_failed_some_family_checks(
        self,
        mock_ff: Mock,
    ) -> None:
        copy_project_pedigree_to_mocked_dir(
            TEST_PEDIGREE_4_REMAP,
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            SampleType.WGS,
            'R0114_project4',
        )
        mock_ff.CHECK_SEX_AND_RELATEDNESS = True
        worker = luigi.worker.Worker()
        wrsc_task = WriteRemappedAndSubsettedCallsetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id=TEST_RUN_ID,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_guids=['R0114_project4'],
            project_i=0,
            skip_validation=True,
            skip_expect_tdr_metrics=True,
        )
        worker.add(wrsc_task)
        worker.run()
        self.assertTrue(wrsc_task.complete())
        mt = hl.read_matrix_table(wrsc_task.output().path)
        # NB: one "family"/"sample" has been removed because of a failed sex check,
        # and 4 removed because of a failed ploidy check!
        self.assertEqual(mt.count(), (30, 8))
        self.assertEqual(
            mt.globals.collect(),
            [
                hl.Struct(
                    remap_pedigree_hash=hl.eval(
                        remap_pedigree_hash(
                            TEST_PEDIGREE_4_REMAP,
                        ),
                    ),
                    family_samples={
                        '123_1': ['NA19675_1'],
                        '345_1': ['NA19679_1'],
                        '456_1': ['NA20870_1'],
                        '678_1': ['NA20874_1'],
                        '789_1': ['NA20875_1'],
                        '890_1': ['NA20876_1'],
                        '901_1': ['NA20877_1'],
                        'efg_1': ['NA20888_1'],
                    },
                    failed_family_samples=hl.Struct(
                        missing_samples={},
                        relatedness_check={},
                        sex_check={
                            'def_1': {
                                'reasons': [
                                    'Sample NA20885_1 has pedigree sex M but imputed sex F',
                                ],
                                'samples': ['NA20885_1'],
                            },
                        },
                        ploidy_check={
                            '234_1': {
                                'reasons': [
                                    "Found samples with misaligned ploidy with their provided imputed sex: ['NA19678_1']",
                                ],
                                'samples': ['NA19678_1'],
                            },
                            '567_1': {
                                'reasons': [
                                    "Found samples with misaligned ploidy with their provided imputed sex: ['NA20872_1']",
                                ],
                                'samples': ['NA20872_1'],
                            },
                            'bcd_1': {
                                'reasons': [
                                    "Found samples with misaligned ploidy with their provided imputed sex: ['NA20878_1']",
                                ],
                                'samples': ['NA20878_1'],
                            },
                            'cde_1': {
                                'reasons': [
                                    "Found samples with misaligned ploidy with their provided imputed sex: ['NA20881_1']",
                                ],
                                'samples': ['NA20881_1'],
                            },
                        },
                    ),
                ),
            ],
        )

    @patch('v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset.FeatureFlag')
    def test_write_remapped_and_subsetted_callset_task_all_families_failed(
        self,
        mock_ff: Mock,
    ) -> None:
        copy_project_pedigree_to_mocked_dir(
            TEST_PEDIGREE_7,
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            SampleType.WGS,
            'R0114_project4',
        )
        mock_ff.CHECK_SEX_AND_RELATEDNESS = True
        worker = luigi.worker.Worker()
        wrsc_task = WriteRemappedAndSubsettedCallsetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id=TEST_RUN_ID,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_guids=['R0114_project4'],
            project_i=0,
            skip_validation=True,
            skip_expect_tdr_metrics=True,
        )
        worker.add(wrsc_task)
        worker.run()
        self.assertFalse(wrsc_task.complete())
        write_validation_errors_task = WriteValidationErrorsForRunTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WES,
            callset_path=TEST_VCF,
            project_guids=['R0114_project4'],
            skip_validation=True,
            run_id=TEST_RUN_ID,
        )
        self.assertTrue(write_validation_errors_task.complete())
        with write_validation_errors_task.output().open('r') as f:
            self.assertDictEqual(
                json.load(f),
                {
                    'project_guids': ['R0114_project4'],
                    'error_messages': ['All families failed validation checks'],
                    'failed_family_samples': {
                        'missing_samples': {
                            'efg_1': {
                                'samples': ['NA99999_1'],
                                'reasons': ["Missing samples: {'NA99999_1'}"],
                            },
                        },
                        'relatedness_check': {},
                        'sex_check': {
                            '890_1': {
                                'samples': ['NA20876_1'],
                                'reasons': [
                                    'Sample NA20876_1 has pedigree sex M but imputed sex F',
                                ],
                            },
                            '456_1': {
                                'samples': ['NA20870_1'],
                                'reasons': [
                                    'Sample NA20870_1 has pedigree sex M but imputed sex F',
                                ],
                            },
                            '123_1': {
                                'samples': ['NA19675_1'],
                                'reasons': [
                                    'Sample NA19675_1 has pedigree sex M but imputed sex F',
                                ],
                            },
                            '678_1': {
                                'samples': ['NA20874_1'],
                                'reasons': [
                                    'Sample NA20874_1 has pedigree sex M but imputed sex F',
                                ],
                            },
                            '789_1': {
                                'samples': ['NA20875_1'],
                                'reasons': [
                                    'Sample NA20875_1 has pedigree sex M but imputed sex F',
                                ],
                            },
                            '901_1': {
                                'samples': ['NA20877_1'],
                                'reasons': [
                                    'Sample NA20877_1 has pedigree sex M but imputed sex F',
                                ],
                            },
                            '345_1': {
                                'samples': ['NA19679_1'],
                                'reasons': [
                                    'Sample NA19679_1 has pedigree sex M but imputed sex F',
                                ],
                            },
                            'def_1': {
                                'samples': ['NA20885_1'],
                                'reasons': [
                                    'Sample NA20885_1 has pedigree sex M but imputed sex F',
                                ],
                            },
                        },
                        'ploidy_check': {
                            '567_1': {
                                'samples': ['NA20872_1'],
                                'reasons': [
                                    "Found samples with misaligned ploidy with their provided imputed sex: ['NA20872_1']",
                                ],
                            },
                            '234_1': {
                                'samples': ['NA19678_1'],
                                'reasons': [
                                    "Found samples with misaligned ploidy with their provided imputed sex: ['NA19678_1']",
                                ],
                            },
                            'bcd_1': {
                                'samples': ['NA20878_1'],
                                'reasons': [
                                    "Found samples with misaligned ploidy with their provided imputed sex: ['NA20878_1']",
                                ],
                            },
                            'cde_1': {
                                'samples': ['NA20881_1'],
                                'reasons': [
                                    "Found samples with misaligned ploidy with their provided imputed sex: ['NA20881_1']",
                                ],
                            },
                        },
                    },
                },
            )
