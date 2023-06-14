import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.tasks.update_sample_lookup_table import (
    UpdateSampleLookupTableTask,
)

TEST_VCF = 'v03_pipeline/var/test/vcfs/1kg_30variants.vcf.bgz'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'
TEST_PEDIGREE_4 = 'v03_pipeline/var/test/pedigrees/test_pedigree_4.tsv'


@patch('v03_pipeline.lib.paths.DataRoot')
class UpdateSampleLookupTableTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_local_datasets = tempfile.TemporaryDirectory().name

    def tearDown(self) -> None:
        if os.path.isdir(self._temp_local_datasets):
            shutil.rmtree(self._temp_local_datasets)

    def test_update_sample_lookup_table_task(self, mock_dataroot: Mock) -> None:
        mock_dataroot.LOCAL_DATASETS.value = self._temp_local_datasets
        worker = luigi.worker.Worker()

        uslt_task = UpdateSampleLookupTableTask(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            callset_path=TEST_VCF,
            project_remap_path=TEST_REMAP,
            project_pedigree_path=TEST_PEDIGREE_3,
            project_guid='R0113_test_project',
        )
        worker.add(uslt_task)
        worker.run()
        self.assertEqual(
            uslt_task.output().path,
            f'{self._temp_local_datasets}/GRCh38/v03/SNV/lookup.ht',
        )
        self.assertTrue(uslt_task.output().exists())
        self.assertTrue(uslt_task.complete())
        ht = hl.read_table(uslt_task.output().path)
        self.assertCountEqual(
            [x for x in ht.collect() if x.locus.position <= 883625],  # noqa: PLR2004
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'C'],
                    ref_samples={'HG00733_1', 'HG00732_1', 'HG00731_1'},
                    het_samples=set(),
                    hom_samples=set(),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=874734,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    ref_samples={'HG00733_1', 'HG00732_1', 'HG00731_1'},
                    het_samples=set(),
                    hom_samples=set(),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=876499,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'G'],
                    ref_samples=set(),
                    het_samples=set(),
                    hom_samples={'HG00732_1', 'HG00731_1', 'HG00733_1'},
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=878314,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'C'],
                    ref_samples={'HG00732_1'},
                    het_samples={'HG00731_1', 'HG00733_1'},
                    hom_samples=set(),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=878809,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    ref_samples={'HG00733_1', 'HG00732_1', 'HG00731_1'},
                    het_samples=set(),
                    hom_samples=set(),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=879576,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    ref_samples={'HG00731_1', 'HG00733_1'},
                    het_samples={'HG00732_1'},
                    hom_samples=set(),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=881627,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    ref_samples={'HG00732_1'},
                    het_samples={'HG00733_1'},
                    hom_samples={'HG00731_1'},
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=881070,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    ref_samples={'HG00733_1', 'HG00732_1', 'HG00731_1'},
                    het_samples=set(),
                    hom_samples=set(),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=881918,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    ref_samples={'HG00733_1', 'HG00732_1', 'HG00731_1'},
                    het_samples=set(),
                    hom_samples=set(),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=883485,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    ref_samples={'HG00733_1', 'HG00732_1', 'HG00731_1'},
                    het_samples=set(),
                    hom_samples=set(),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=883625,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'G'],
                    ref_samples=set(),
                    het_samples=set(),
                    hom_samples={'HG00732_1', 'HG00731_1', 'HG00733_1'},
                ),
            ],
        )
