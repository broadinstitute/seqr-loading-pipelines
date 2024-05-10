import shutil
from unittest import mock

import hail as hl
import luigi.worker

from v03_pipeline.lib.misc.io import import_vcf
from v03_pipeline.lib.model import (
    CachedReferenceDatasetQuery,
    DatasetType,
    ReferenceGenome,
    SampleType,
)
from v03_pipeline.lib.paths import (
    imported_callset_path,
    relatedness_check_table_path,
    valid_cached_reference_dataset_query_path,
)
from v03_pipeline.lib.tasks.write_relatedness_check_table import (
    WriteRelatednessCheckTableTask,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_GNOMAD_QC_HT = 'v03_pipeline/var/test/reference_data/gnomad_qc_crdq.ht'
TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'

MOCK_CONFIG = {
    'gnomad_qc': {
        '38': {
            'version': '4.0',
            'source_path': TEST_GNOMAD_QC_HT,
            'custom_import': lambda *_: hl.Table.parallelize(
                [],
                hl.tstruct(
                    locus=hl.tlocus('GRCh38'),
                    alleles=hl.tarray(hl.tstr),
                ),
                key=['locus', 'alleles'],
            ),
        },
    },
}


class WriteRelatednessCheckTableTaskTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.gnomad_qc_path = valid_cached_reference_dataset_query_path(
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            CachedReferenceDatasetQuery.GNOMAD_QC,
        )
        shutil.copytree(
            TEST_GNOMAD_QC_HT,
            self.gnomad_qc_path,
        )

        # Force imported callset to be complete
        ht = import_vcf(TEST_VCF, ReferenceGenome.GRCh38)
        ht = ht.annotate_globals(sample_type=SampleType.WGS.value)
        ht.write(
            imported_callset_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_VCF,
            ),
        )

    @mock.patch.dict(
        'v03_pipeline.lib.reference_data.compare_globals.CONFIG',
        MOCK_CONFIG,
    )
    @mock.patch.dict(
        'v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query.CONFIG',
        MOCK_CONFIG,
    )
    def test_relatedness_check_table_task(
        self,
    ) -> None:
        ht = hl.read_table(
            self.gnomad_qc_path,
        )
        self.assertEqual(
            hl.eval(ht.versions.gnomad_qc),
            'v3.1',
        )
        worker = luigi.worker.Worker()
        task = WriteRelatednessCheckTableTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())
        ht = hl.read_table(self.gnomad_qc_path)
        self.assertEqual(
            hl.eval(ht.versions.gnomad_qc),
            '4.0',
        )
        ht = hl.read_table(
            relatedness_check_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_VCF,
            ),
        )
        self.assertEqual(
            ht.collect(),
            [],
        )
