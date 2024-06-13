import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.update_lookup_table import (
    UpdateLookupTableTask,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'


class UpdateLookupTableTest(MockedDatarootTestCase):
    def test_skip_update_lookup_table_task(self) -> None:
        worker = luigi.worker.Worker()
        uslt_task = UpdateLookupTableTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_guids=[
                'R0555_seqr_demo',
            ],  # a project excluded from the lookup table
            project_remap_paths=[TEST_REMAP],
            project_pedigree_paths=[TEST_PEDIGREE_3],
            validate=False,
        )
        worker.add(uslt_task)
        worker.run()
        self.assertTrue(uslt_task.output().exists())
        self.assertTrue(uslt_task.complete())
        ht = hl.read_table(uslt_task.output().path)
        self.assertEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    project_guids=[],
                    project_families={},
                    updates={
                        hl.Struct(callset=TEST_VCF, project_guid='R0555_seqr_demo'),
                    },
                ),
            ],
        )
        self.assertEqual(ht.collect(), [])

    def test_update_lookup_table_task(self) -> None:
        worker = luigi.worker.Worker()
        uslt_task = UpdateLookupTableTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_guids=['R0113_test_project'],
            project_remap_paths=[TEST_REMAP],
            project_pedigree_paths=[TEST_PEDIGREE_3],
            validate=False,
        )
        worker.add(uslt_task)
        worker.run()
        self.assertTrue(uslt_task.output().exists())
        self.assertTrue(uslt_task.complete())
        ht = hl.read_table(uslt_task.output().path)
        self.assertEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    project_guids=['R0113_test_project'],
                    project_families={'R0113_test_project': ['abc_1']},
                    updates={
                        hl.Struct(callset=TEST_VCF, project_guid='R0113_test_project'),
                    },
                ),
            ],
        )
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
                    project_stats=[
                        [hl.Struct(ref_samples=3, het_samples=0, hom_samples=0)],
                    ],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=874734,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    project_stats=[
                        [hl.Struct(ref_samples=3, het_samples=0, hom_samples=0)],
                    ],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=876499,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'G'],
                    project_stats=[
                        [hl.Struct(ref_samples=0, het_samples=0, hom_samples=3)],
                    ],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=878314,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'C'],
                    project_stats=[
                        [hl.Struct(ref_samples=1, het_samples=2, hom_samples=0)],
                    ],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=878809,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    project_stats=[
                        [hl.Struct(ref_samples=3, het_samples=0, hom_samples=0)],
                    ],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=879576,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    project_stats=[
                        [hl.Struct(ref_samples=2, het_samples=1, hom_samples=0)],
                    ],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=881627,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    project_stats=[
                        [hl.Struct(ref_samples=1, het_samples=1, hom_samples=1)],
                    ],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=881070,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    project_stats=[
                        [hl.Struct(ref_samples=3, het_samples=0, hom_samples=0)],
                    ],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=881918,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    project_stats=[
                        [hl.Struct(ref_samples=3, het_samples=0, hom_samples=0)],
                    ],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=883485,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    project_stats=[
                        [hl.Struct(ref_samples=3, het_samples=0, hom_samples=0)],
                    ],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=883625,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'G'],
                    project_stats=[
                        [hl.Struct(ref_samples=0, het_samples=0, hom_samples=3)],
                    ],
                ),
            ],
        )
