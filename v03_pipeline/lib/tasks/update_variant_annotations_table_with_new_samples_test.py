import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import hail as hl
import luigi.worker

from v03_pipeline.lib.definitions import DatasetType, Env, ReferenceGenome, SampleType
from v03_pipeline.lib.misc.io import write_ht
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
            vcf_path=TEST_VCF,
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
            vcf_path=TEST_VCF,
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
            vcf_path=TEST_VCF,
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
            vcf_path=TEST_VCF,
            project_remap_path=TEST_REMAP,
            project_pedigree_path=TEST_PEDIGREE_5,
        )
        worker.add(uvatwns_task_5)
        worker.run()
        self.assertTrue(uvatwns_task_5.complete())
        ht = hl.read_table(uvatwns_task_5.output().path)
        self.assertEqual(ht.count(), 30)
        self.assertCountEqual(
            ht.select('variant_id', 'pos', 'original_alt_alleles').collect(),
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'C'],
                    variant_id='1-871269-A-C',
                    pos=871269,
                    original_alt_alleles=['chr1-871269-A-C'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=874734,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    variant_id='1-874734-C-T',
                    pos=874734,
                    original_alt_alleles=['chr1-874734-C-T'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=876499,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'G'],
                    variant_id='1-876499-A-G',
                    pos=876499,
                    original_alt_alleles=['chr1-876499-A-G'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=878314,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'C'],
                    variant_id='1-878314-G-C',
                    pos=878314,
                    original_alt_alleles=['chr1-878314-G-C'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=878809,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    variant_id='1-878809-C-T',
                    pos=878809,
                    original_alt_alleles=['chr1-878809-C-T'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=879576,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    variant_id='1-879576-C-T',
                    pos=879576,
                    original_alt_alleles=['chr1-879576-C-T'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=881070,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    variant_id='1-881070-G-A',
                    pos=881070,
                    original_alt_alleles=['chr1-881070-G-A'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=881627,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    variant_id='1-881627-G-A',
                    pos=881627,
                    original_alt_alleles=['chr1-881627-G-A'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=881918,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    variant_id='1-881918-G-A',
                    pos=881918,
                    original_alt_alleles=['chr1-881918-G-A'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=883485,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    variant_id='1-883485-C-T',
                    pos=883485,
                    original_alt_alleles=['chr1-883485-C-T'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=883625,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'G'],
                    variant_id='1-883625-A-G',
                    pos=883625,
                    original_alt_alleles=['chr1-883625-A-G'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=883918,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    variant_id='1-883918-G-A',
                    pos=883918,
                    original_alt_alleles=['chr1-883918-G-A'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=887560,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'C'],
                    variant_id='1-887560-A-C',
                    pos=887560,
                    original_alt_alleles=['chr1-887560-A-C'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=887801,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'G'],
                    variant_id='1-887801-A-G',
                    pos=887801,
                    original_alt_alleles=['chr1-887801-A-G'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=888529,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    variant_id='1-888529-G-A',
                    pos=888529,
                    original_alt_alleles=['chr1-888529-G-A'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=888659,
                        reference_genome='GRCh38',
                    ),
                    alleles=['T', 'C'],
                    variant_id='1-888659-T-C',
                    pos=888659,
                    original_alt_alleles=['chr1-888659-T-C'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=889158,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'C'],
                    variant_id='1-889158-G-C',
                    pos=889158,
                    original_alt_alleles=['chr1-889158-G-C'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=889159,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'C'],
                    variant_id='1-889159-A-C',
                    pos=889159,
                    original_alt_alleles=['chr1-889159-A-C'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=889238,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    variant_id='1-889238-G-A',
                    pos=889238,
                    original_alt_alleles=['chr1-889238-G-A'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=894573,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    variant_id='1-894573-G-A',
                    pos=894573,
                    original_alt_alleles=['chr1-894573-G-A'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=896922,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    variant_id='1-896922-C-T',
                    pos=896922,
                    original_alt_alleles=['chr1-896922-C-T'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=897325,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'C'],
                    variant_id='1-897325-G-C',
                    pos=897325,
                    original_alt_alleles=['chr1-897325-G-C'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=898313,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    variant_id='1-898313-C-T',
                    pos=898313,
                    original_alt_alleles=['chr1-898313-C-T'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=898323,
                        reference_genome='GRCh38',
                    ),
                    alleles=['T', 'C'],
                    variant_id='1-898323-T-C',
                    pos=898323,
                    original_alt_alleles=['chr1-898323-T-C'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=898467,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    variant_id='1-898467-C-T',
                    pos=898467,
                    original_alt_alleles=['chr1-898467-C-T'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=899959,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'GC'],
                    variant_id='1-899959-G-GC',
                    pos=899959,
                    original_alt_alleles=['chr1-899959-G-GC'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=900505,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'C'],
                    variant_id='1-900505-G-C',
                    pos=900505,
                    original_alt_alleles=['chr1-900505-G-C'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=902024,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    variant_id='1-902024-G-A',
                    pos=902024,
                    original_alt_alleles=['chr1-902024-G-A'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=902069,
                        reference_genome='GRCh38',
                    ),
                    alleles=['T', 'C'],
                    variant_id='1-902069-T-C',
                    pos=902069,
                    original_alt_alleles=['chr1-902069-T-C'],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=902088,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    variant_id='1-902088-G-A',
                    pos=902088,
                    original_alt_alleles=['chr1-902088-G-A'],
                ),
            ],
        )
