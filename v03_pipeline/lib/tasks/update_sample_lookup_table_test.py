import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.update_sample_lookup_table import (
    UpdateSampleLookupTableTask,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'


class UpdateSampleLookupTableTest(MockedDatarootTestCase):
    def test_update_sample_lookup_table_task(self) -> None:
        worker = luigi.worker.Worker()
        uslt_task = UpdateSampleLookupTableTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_guid='R0113_test_project',
            project_remap_path=TEST_REMAP,
            project_pedigree_path=TEST_PEDIGREE_3,
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
                    ref_samples=hl.Struct(
                        R0113_test_project={'HG00733_1', 'HG00732_1', 'HG00731_1'},
                    ),
                    het_samples=hl.Struct(R0113_test_project=set()),
                    hom_samples=hl.Struct(R0113_test_project=set()),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=874734,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    ref_samples=hl.Struct(
                        R0113_test_project={'HG00733_1', 'HG00732_1', 'HG00731_1'},
                    ),
                    het_samples=hl.Struct(R0113_test_project=set()),
                    hom_samples=hl.Struct(R0113_test_project=set()),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=876499,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'G'],
                    ref_samples=hl.Struct(R0113_test_project=set()),
                    het_samples=hl.Struct(R0113_test_project=set()),
                    hom_samples=hl.Struct(
                        R0113_test_project={'HG00733_1', 'HG00732_1', 'HG00731_1'},
                    ),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=878314,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'C'],
                    ref_samples=hl.Struct(R0113_test_project={'HG00732_1'}),
                    het_samples=hl.Struct(
                        R0113_test_project={'HG00731_1', 'HG00733_1'},
                    ),
                    hom_samples=hl.Struct(R0113_test_project=set()),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=878809,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    ref_samples=hl.Struct(
                        R0113_test_project={'HG00733_1', 'HG00732_1', 'HG00731_1'},
                    ),
                    het_samples=hl.Struct(R0113_test_project=set()),
                    hom_samples=hl.Struct(R0113_test_project=set()),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=879576,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    ref_samples=hl.Struct(
                        R0113_test_project={'HG00731_1', 'HG00733_1'},
                    ),
                    het_samples=hl.Struct(R0113_test_project={'HG00732_1'}),
                    hom_samples=hl.Struct(R0113_test_project=set()),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=881627,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    ref_samples=hl.Struct(R0113_test_project={'HG00732_1'}),
                    het_samples=hl.Struct(R0113_test_project={'HG00733_1'}),
                    hom_samples=hl.Struct(R0113_test_project={'HG00731_1'}),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=881070,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    ref_samples=hl.Struct(
                        R0113_test_project={'HG00733_1', 'HG00732_1', 'HG00731_1'},
                    ),
                    het_samples=hl.Struct(R0113_test_project=set()),
                    hom_samples=hl.Struct(R0113_test_project=set()),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=881918,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'A'],
                    ref_samples=hl.Struct(
                        R0113_test_project={'HG00733_1', 'HG00732_1', 'HG00731_1'},
                    ),
                    het_samples=hl.Struct(R0113_test_project=set()),
                    hom_samples=hl.Struct(R0113_test_project=set()),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=883485,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    ref_samples=hl.Struct(
                        R0113_test_project={'HG00733_1', 'HG00732_1', 'HG00731_1'},
                    ),
                    het_samples=hl.Struct(R0113_test_project=set()),
                    hom_samples=hl.Struct(R0113_test_project=set()),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=883625,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'G'],
                    ref_samples=hl.Struct(R0113_test_project=set()),
                    het_samples=hl.Struct(R0113_test_project=set()),
                    hom_samples=hl.Struct(
                        R0113_test_project={'HG00733_1', 'HG00732_1', 'HG00731_1'},
                    ),
                ),
            ],
        )
