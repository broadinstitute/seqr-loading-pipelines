from unittest import mock
from unittest.mock import Mock

import hail as hl
import luigi.worker
import pandas as pd

from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
    SampleType,
)
from v03_pipeline.lib.paths import (
    new_clinvar_variants_parquet_path,
    new_variants_table_path,
)
from v03_pipeline.lib.tasks.exports.write_new_clinvar_variants_parquet import (
    WriteNewClinvarVariantsParquetTask,
)
from v03_pipeline.lib.test.misc import convert_ndarray_to_list
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_SNV_INDEL_ANNOTATIONS = (
    'v03_pipeline/var/test/exports/GRCh38/SNV_INDEL/annotations.ht'
)
TEST_MITO_ANNOTATIONS = 'v03_pipeline/var/test/exports/GRCh38/MITO/annotations.ht'

TEST_RUN_ID = 'manual__2024-04-03'


class WriteNewClinvarVariantsParquetTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        ht = hl.read_table(
            TEST_SNV_INDEL_ANNOTATIONS,
        )
        ht.write(
            new_variants_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        ht = hl.read_table(
            TEST_MITO_ANNOTATIONS,
        )
        ht.write(
            new_variants_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.MITO,
                TEST_RUN_ID,
            ),
        )

    def test_write_new_clinvar_variants_parquet_test(
        self,
    ) -> None:
        worker = luigi.worker.Worker()
        task = WriteNewClinvarVariantsParquetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path='fake_callset',
            project_guids=[
                'fake_project',
            ],
            project_pedigree_paths=['fake_pedigree'],
            skip_validation=True,
            run_id=TEST_RUN_ID,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        df = pd.read_parquet(
            new_clinvar_variants_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        export_json = convert_ndarray_to_list(df.head(1).to_dict('records'))
        self.assertEqual(
            export_json,
            [
                {
                    'key': 0,
                    'alleleId': 929885,
                    'conflictingPathogenicities': None,
                    'goldStars': 1,
                    'submitters': ['Labcorp Genetics (formerly Invitae), Labcorp'],
                    'conditions': ['not provided'],
                    'assertions': [],
                    'pathogenicity': 'Uncertain_significance',
                },
            ],
        )

    @mock.patch(
        'v03_pipeline.lib.tasks.exports.write_new_clinvar_variants_parquet.WriteNewVariantsTableTask',
    )
    def test_write_new_mito_clinvar_variants_parquet_test(
        self,
        write_new_variants_table_task: Mock,
    ) -> None:
        write_new_variants_table_task.return_value = MockCompleteTask()
        worker = luigi.worker.Worker()
        task = WriteNewClinvarVariantsParquetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.MITO,
            sample_type=SampleType.WGS,
            callset_path='fake_callset',
            project_guids=[
                'fake_project',
            ],
            project_pedigree_paths=['fake_pedigree'],
            skip_validation=True,
            run_id=TEST_RUN_ID,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        df = pd.read_parquet(
            new_clinvar_variants_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.MITO,
                TEST_RUN_ID,
            ),
        )
        export_json = convert_ndarray_to_list(df.head(1).to_dict('records'))
        self.assertEqual(
            export_json,
            [
                {
                    'key': 998,
                    'alleleId': 677824,
                    'conflictingPathogenicities': None,
                    'goldStars': 1,
                    'submitters': [
                        'Wong Mito Lab, Molecular and Human Genetics, Baylor College of Medicine',
                    ],
                    'conditions': ['MELAS syndrome'],
                    'assertions': [],
                    'pathogenicity': 'Likely_pathogenic',
                },
            ],
        )
