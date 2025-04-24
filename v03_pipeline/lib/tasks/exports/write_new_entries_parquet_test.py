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

TEST_RUN_ID = 'manual__2024-04-03'

TEST_PEDIGREE_3_REMAP = 'v03_pipeline/var/test/pedigrees/test_pedigree_3_remap.tsv'
TEST_PEDIGREE_4_REMAP = 'v03_pipeline/var/test/pedigrees/test_pedigree_4_remap.tsv'
TEST_SNV_INDEL_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'


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
        export_json = convert_ndarray_to_list(df.head(1).to_dict('records'))
        self.assertListEqual(list(export_json[0].keys()), ['key', 'transcripts'])
        self.assertEqual(
            export_json[0]['key'],
            0,
        )
