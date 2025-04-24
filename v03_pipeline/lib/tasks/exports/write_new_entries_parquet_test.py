import os

import hail as hl
import luigi.worker
import pandas as pd

from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
    SampleType,
)
from v03_pipeline.lib.paths import (
    new_entries_parquet_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.exports.write_new_entries_parquet import (
    WriteNewEntriesParquetTask,
)
from v03_pipeline.lib.test.misc import convert_ndarray_to_list
from v03_pipeline.lib.test.mocked_reference_datasets_testcase import (
    MockedReferenceDatasetsTestCase,
)
from v03_pipeline.lib.misc.io import import_vcf, remap_pedigree_hash

TEST_PEDIGREE_3_REMAP = 'v03_pipeline/var/test/pedigrees/test_pedigree_3_remap.tsv'
TEST_PEDIGREE_4_REMAP = 'v03_pipeline/var/test/pedigrees/test_pedigree_4_remap.tsv'
TEST_SNV_INDEL_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'

TEST_RUN_ID = 'manual__2024-04-03'


class WriteNewEntriesParquetTest(MockedReferenceDatasetsTestCase):
    def setUp(self) -> None:
        super().setUp()
        mt = import_vcf(TEST_SNV_INDEL_VCF, ReferenceGenome.GRCh38)
        ht = mt.rows()
        ht = ht.add_index(name='key_')
        ht = ht.annotate_globals(
            updates={
                hl.Struct(
                    callset=TEST_SNV_INDEL_VCF,
                    project_guid='R0113_test_project',
                    remap_pedigree_hash=remap_pedigree_hash(TEST_PEDIGREE_3_REMAP),
                ),
                hl.Struct(
                    callset=TEST_SNV_INDEL_VCF,
                    project_guid='R0114_project4',
                    remap_pedigree_hash=remap_pedigree_hash(TEST_PEDIGREE_4_REMAP),
                ),
            }
        )
        ht.write(
            variant_annotations_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
            ),
        )

    def test_write_new_entries_parquet(self):
        worker = luigi.worker.Worker()
        task = WriteNewEntriesParquetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path=TEST_SNV_INDEL_VCF,
            project_guids=['R0113_test_project', 'R0114_project4'],
            project_pedigree_paths=[TEST_PEDIGREE_3_REMAP, TEST_PEDIGREE_4_REMAP],
            skip_validation=True,
            run_id=TEST_RUN_ID,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        df = pd.read_parquet(
            os.path.join(
                new_entries_parquet_path(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    TEST_RUN_ID,
                ),
            ),
        )
        export_json = convert_ndarray_to_list(df.to_dict('records'))
        self.assertEqual(
            export_json[:2],
            [
                {
                    'key': 2,
                    'project_guid': 'R0113_test_project',
                    'family_guid': 'abc_1',
                    'sample_type': 'WGS',
                    'xpos': 1000876499,
                    'is_gnomad_gt_5_percent': False,
                    'filters': [],
                    'calls': {
                        'sampleId': ['HG00731_1', 'HG00732_1', 'HG00733_1'],
                        'gt': [2, 2, 2],
                        'gq': [21, 24, 12],
                        'ab': [1.0, 1.0, 1.0],
                        'dp': [7, 8, 4],
                    },
                },
                {
                    'key': 3,
                    'project_guid': 'R0113_test_project',
                    'family_guid': 'abc_1',
                    'sample_type': 'WGS',
                    'xpos': 1000878314,
                    'is_gnomad_gt_5_percent': False,
                    'filters': ['VQSRTrancheSNP99.00to99.90'],
                    'calls': {
                        'sampleId': ['HG00731_1', 'HG00732_1', 'HG00733_1'],
                        'gt': [1, 0, 1],
                        'gq': [30, 6, 61],
                        'ab': [0.3333333432674408, 0.0, 0.6000000238418579],
                        'dp': [3, 2, 5],
                    },
                },
            ],
        )
