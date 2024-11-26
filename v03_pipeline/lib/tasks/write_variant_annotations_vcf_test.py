from unittest.mock import Mock, patch

import hailtop.fs as hfs
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.update_variant_annotations_table_with_new_samples import (
    UpdateVariantAnnotationsTableWithNewSamplesTask,
)
from v03_pipeline.lib.tasks.write_variant_annotations_vcf import (
    WriteVariantAnnotationsVCF,
)
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_SV_VCF = 'v03_pipeline/var/test/callsets/sv_1.vcf'
TEST_PEDIGREE_5 = 'v03_pipeline/var/test/pedigrees/test_pedigree_5.tsv'

GENE_ID_MAPPING = {
    'OR4F5': 'ENSG00000186092',
    'PLEKHG4B': 'ENSG00000153404',
    'OR4F16': 'ENSG00000186192',
    'OR4F29': 'ENSG00000284733',
    'FBXO28': 'ENSG00000143756',
    'SAMD11': 'ENSG00000187634',
    'C1orf174': 'ENSG00000198912',
    'TAS1R1': 'ENSG00000173662',
    'FAM131C': 'ENSG00000185519',
    'RCC2': 'ENSG00000179051',
    'NBPF3': 'ENSG00000142794',
    'AGBL4': 'ENSG00000186094',
    'KIAA1614': 'ENSG00000135835',
    'MR1': 'ENSG00000153029',
    'STX6': 'ENSG00000135823',
    'XPR1': 'ENSG00000143324',
}


class WriteVariantAnnotationsVCFTest(MockedDatarootTestCase):
    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.load_gencode_gene_symbol_to_gene_id',
    )
    @patch(
        'v03_pipeline.lib.tasks.base.base_update_variant_annotations_table.UpdatedReferenceDatasetQueryTask',
    )
    def test_sv_export_vcf(
        self,
        mock_rd_query_task: Mock,
        mock_load_gencode: Mock,
    ) -> None:
        mock_load_gencode.return_value = GENE_ID_MAPPING
        mock_rd_query_task.return_value = MockCompleteTask()
        worker = luigi.worker.Worker()
        update_variant_annotations_task = (
            UpdateVariantAnnotationsTableWithNewSamplesTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SV,
                run_id='run_id1',
                sample_type=SampleType.WGS,
                callset_path=TEST_SV_VCF,
                project_guids=['R0115_test_project2'],
                project_remap_paths=['not_a_real_file'],
                project_pedigree_paths=[TEST_PEDIGREE_5],
                skip_validation=True,
            )
        )
        worker.add(update_variant_annotations_task)
        worker.run()
        self.assertTrue(update_variant_annotations_task.complete())
        write_variant_annotations_vcf_task = WriteVariantAnnotationsVCF(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SV,
            run_id='run_id1',
            sample_type=SampleType.WGS,
            callset_path=TEST_SV_VCF,
        )
        worker.add(write_variant_annotations_vcf_task)
        worker.run()
        self.assertTrue(
            hfs.exists(
                write_variant_annotations_vcf_task.output().path,
            ),
        )
