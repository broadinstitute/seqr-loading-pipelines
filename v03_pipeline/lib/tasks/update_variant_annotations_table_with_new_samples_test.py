import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import hail as hl
import luigi.worker

from v03_pipeline.lib.misc.io import write_ht
from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.update_variant_annotations_table_with_new_samples import (
    UpdateVariantAnnotationsTableWithNewSamples,
)

TEST_VCF = 'v03_pipeline/var/test/vcfs/1kg_30variants.vcf.bgz'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'
TEST_PEDIGREE_4 = 'v03_pipeline/var/test/pedigrees/test_pedigree_4.tsv'
TEST_PEDIGREE_5 = 'v03_pipeline/var/test/pedigrees/test_pedigree_5.tsv'


@patch('v03_pipeline.lib.paths.DataRoot')
class UpdateVariantAnnotationsTableWithNewSamplesTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_local_datasets = tempfile.TemporaryDirectory().name
        self._temp_local_reference_data = tempfile.TemporaryDirectory().name
        write_ht(
            env=Env.LOCAL,
            ht=hl.Table.parallelize(
                [
                    {
                        'locus': hl.Locus(
                            contig='chr1',
                            position=871269,
                            reference_genome='GRCh38',
                        ),
                        'alleles': ['A', 'C'],
                        'cadd': 1,
                        'clinvar': 2,
                    },
                ],
                hl.tstruct(
                    locus=hl.tlocus('GRCh38'),
                    alleles=hl.tarray(hl.tstr),
                    cadd=hl.tint32,
                    clinvar=hl.tint32,
                ),
                ['locus', 'alleles'],
            ),
            destination_path=os.path.join(
                f'{self._temp_local_reference_data}/GRCh38/v03/combined.ht',
            ),
            checkpoint=False,
        )

    def tearDown(self) -> None:
        if os.path.isdir(self._temp_local_datasets):
            shutil.rmtree(self._temp_local_datasets)

        if os.path.isdir(self._temp_local_reference_data):
            shutil.rmtree(self._temp_local_reference_data)

    def test_missing_pedigree(self, mock_dataroot: Mock) -> None:
        mock_dataroot.LOCAL_DATASETS.value = self._temp_local_datasets
        mock_dataroot.LOCAL_REFERENCE_DATA.value = self._temp_local_reference_data
        uvatwns_task = UpdateVariantAnnotationsTableWithNewSamples(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_remap_path=TEST_REMAP,
            project_pedigree_path='bad_pedigree',
        )

        worker = luigi.worker.Worker()
        worker.add(uvatwns_task)
        worker.run()
        self.assertFalse(uvatwns_task.complete())

    def test_mulitiple_update_vat(self, mock_dataroot: Mock) -> None:
        mock_dataroot.LOCAL_DATASETS.value = self._temp_local_datasets
        mock_dataroot.LOCAL_REFERENCE_DATA.value = self._temp_local_reference_data
        worker = luigi.worker.Worker()

        uvatwns_task_3 = UpdateVariantAnnotationsTableWithNewSamples(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_remap_path=TEST_REMAP,
            project_pedigree_path=TEST_PEDIGREE_3,
        )

        worker.add(uvatwns_task_3)
        worker.run()
        self.assertTrue(uvatwns_task_3.complete())
        self.assertEqual(
            hl.read_table(uvatwns_task_3.output().path).count(),
            17,
        )

        # Ensure that new variants are added correctly to the table.
        uvatwns_task_4 = UpdateVariantAnnotationsTableWithNewSamples(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_remap_path=TEST_REMAP,
            project_pedigree_path=TEST_PEDIGREE_4,
        )
        worker.add(uvatwns_task_4)
        worker.run()
        self.assertTrue(uvatwns_task_4.complete())
        self.assertEqual(
            hl.read_table(uvatwns_task_4.output().path).count(),
            30,
        )

        # If there are no new variants, ensure nothing happens.
        uvatwns_task_5 = UpdateVariantAnnotationsTableWithNewSamples(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_remap_path=TEST_REMAP,
            project_pedigree_path=TEST_PEDIGREE_5,
        )
        worker.add(uvatwns_task_5)
        worker.run()
        self.assertTrue(uvatwns_task_5.complete())
        self.assertEqual(
            hl.read_table(uvatwns_task_5.output().path).count(),
            30,
        )
        self.assertEqual(
            hl.read_table(uvatwns_task_5.output().path).count(),
            30,
        )
