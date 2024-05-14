import shutil

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
            check_sex_and_relatedness=True,
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
                    failed_family_samples=hl.Struct(
                        missing_samples={},
                        relatedness_check={},
                        sex_check={},
                    ),
                    family_samples={'abc_1': ['HG00731_1', 'HG00732_1', 'HG00733_1']},
                ),
            ],
        )

    def test_write_remapped_and_subsetted_callset_task_failed_sex_check_family(
        self,
    ) -> None:
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
            check_sex_and_relatedness=True,
        )
        worker.add(wrsc_task)
        worker.run()
        self.assertTrue(wrsc_task.complete())
        mt = hl.read_matrix_table(wrsc_task.output().path)
        # NB: one "family"/"sample" has been removed because of a failed sex check!
        self.assertEqual(mt.count(), (30, 12))
        self.assertEqual(
            mt.globals.collect(),
            [
                hl.Struct(
                    family_samples={
                        '123_1': ['NA19675_1'],
                        '234_1': ['NA19678_1'],
                        '345_1': ['NA19679_1'],
                        '456_1': ['NA20870_1'],
                        '567_1': ['NA20872_1'],
                        '678_1': ['NA20874_1'],
                        '789_1': ['NA20875_1'],
                        '890_1': ['NA20876_1'],
                        '901_1': ['NA20877_1'],
                        'bcd_1': ['NA20878_1'],
                        'cde_1': ['NA20881_1'],
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
                    ),
                ),
            ],
        )
