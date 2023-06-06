import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.tasks.update_variant_annotations_table_with_new_samples import (
    UpdateVariantAnnotationsTableWithNewSamplesTask,
)

TEST_VCF = 'v03_pipeline/var/test/vcfs/1kg_30variants.vcf.bgz'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'
TEST_PEDIGREE_4 = 'v03_pipeline/var/test/pedigrees/test_pedigree_4.tsv'
TEST_COMBINED_1 = 'v03_pipeline/var/test/reference_data/test_combined_1.ht'
TEST_HGMD_1 = 'v03_pipeline/var/test/reference_data/test_hgmd_1.ht'
TEST_INTERVAL_REFERENCE_1 = (
    'v03_pipeline/var/test/reference_data/test_interval_reference_1.ht'
)


@patch('v03_pipeline.lib.paths.DataRoot')
class UpdateVariantAnnotationsTableWithNewSamplesTaskTest(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self._temp_local_datasets = tempfile.TemporaryDirectory().name
        self._temp_local_reference_data = tempfile.TemporaryDirectory().name
        shutil.copytree(
            TEST_COMBINED_1,
            f'{self._temp_local_reference_data}/GRCh38/v03/combined.ht',
        )
        shutil.copytree(
            TEST_HGMD_1,
            f'{self._temp_local_reference_data}/GRCh38/v03/hgmd.ht',
        )

    def tearDown(self) -> None:
        if os.path.isdir(self._temp_local_datasets):
            shutil.rmtree(self._temp_local_datasets)

        if os.path.isdir(self._temp_local_reference_data):
            shutil.rmtree(self._temp_local_reference_data)

    def test_missing_pedigree(self, mock_dataroot: Mock) -> None:
        mock_dataroot.LOCAL_DATASETS.value = self._temp_local_datasets
        mock_dataroot.LOCAL_REFERENCE_DATA.value = self._temp_local_reference_data
        uvatwns_task = UpdateVariantAnnotationsTableWithNewSamplesTask(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            callset_path=TEST_VCF,
            project_remap_path=TEST_REMAP,
            project_pedigree_path='bad_pedigree',
        )

        worker = luigi.worker.Worker()
        worker.add(uvatwns_task)
        worker.run()
        self.assertFalse(uvatwns_task.complete())

    def test_missing_interval_reference(self, mock_dataroot: Mock) -> None:
        mock_dataroot.LOCAL_DATASETS.value = self._temp_local_datasets
        mock_dataroot.LOCAL_REFERENCE_DATA.value = self._temp_local_reference_data
        uvatwns_task = UpdateVariantAnnotationsTableWithNewSamplesTask(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            callset_path=TEST_VCF,
            project_remap_path=TEST_REMAP,
            project_pedigree_path=TEST_PEDIGREE_3,
        )

        worker = luigi.worker.Worker()
        worker.add(uvatwns_task)
        worker.run()
        self.assertFalse(uvatwns_task.complete())

    def test_mulitiple_update_vat(self, mock_dataroot: Mock) -> None:
        shutil.copytree(
            TEST_INTERVAL_REFERENCE_1,
            f'{self._temp_local_reference_data}/GRCh38/v03/interval_reference.ht',
        )
        mock_dataroot.LOCAL_DATASETS.value = self._temp_local_datasets
        mock_dataroot.LOCAL_REFERENCE_DATA.value = self._temp_local_reference_data
        worker = luigi.worker.Worker()

        uvatwns_task_3 = UpdateVariantAnnotationsTableWithNewSamplesTask(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            callset_path=TEST_VCF,
            project_remap_path=TEST_REMAP,
            project_pedigree_path=TEST_PEDIGREE_3,
        )
        worker.add(uvatwns_task_3)
        worker.run()
        self.assertTrue(uvatwns_task_3.complete())
        ht = hl.read_table(uvatwns_task_3.output().path)
        self.assertEqual(ht.count(), 30)
        self.assertEqual(
            [
                x
                for x in ht.select(
                    'AC',
                    'AN',
                ).collect()
                if x.locus.position <= 871269  # noqa: PLR2004
            ][0],
            hl.Struct(
                locus=hl.Locus(
                    contig='chr1',
                    position=871269,
                    reference_genome='GRCh38',
                ),
                alleles=['A', 'C'],
                AC=0,
                AN=6,
            ),
        )

        # Ensure that new variants are added correctly to the table.
        uvatwns_task_4 = UpdateVariantAnnotationsTableWithNewSamplesTask(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            callset_path=TEST_VCF,
            project_remap_path=TEST_REMAP,
            project_pedigree_path=TEST_PEDIGREE_4,
        )
        worker.add(uvatwns_task_4)
        worker.run()
        self.assertTrue(uvatwns_task_4.complete())
        ht = hl.read_table(uvatwns_task_4.output().path)
        self.assertCountEqual(
            [
                x
                for x in ht.select(
                    'cadd',
                    'clinvar',
                    'hgmd',
                    'variant_id',
                    'xpos',
                    'AC',
                    'AN',
                ).collect()
                if x.locus.position <= 878809  # noqa: PLR2004
            ],
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'C'],
                    cadd=1,
                    clinvar=2,
                    hgmd=hl.Struct(
                        accession='abcdefg',
                        class_id=3,
                    ),
                    variant_id='1-871269-A-C',
                    xpos=1000871269,
                    AC=1,
                    AN=32,
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=874734,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    cadd=None,
                    clinvar=None,
                    hgmd=None,
                    variant_id='1-874734-C-T',
                    xpos=1000874734,
                    AC=1,
                    AN=32,
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=876499,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'G'],
                    cadd=None,
                    clinvar=None,
                    hgmd=None,
                    variant_id='1-876499-A-G',
                    xpos=1000876499,
                    AC=31,
                    AN=32,
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=878314,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'C'],
                    cadd=None,
                    clinvar=None,
                    hgmd=None,
                    variant_id='1-878314-G-C',
                    xpos=1000878314,
                    AC=3,
                    AN=32,
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=878809,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    cadd=None,
                    clinvar=None,
                    hgmd=None,
                    variant_id='1-878809-C-T',
                    xpos=1000878809,
                    AC=1,
                    AN=32,
                ),
            ],
        )
