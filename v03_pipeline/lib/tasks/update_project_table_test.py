import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.update_project_table import UpdateProjectTableTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'


class UpdateProjectTableTaskTest(MockedDatarootTestCase):
    def test_update_project_table_task(self) -> None:
        worker = luigi.worker.Worker()
        upt_task = UpdateProjectTableTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_guid='R0113_test_project',
            project_remap_path=TEST_REMAP,
            project_pedigree_path=TEST_PEDIGREE_3,
            validate=False,
        )
        worker.add(upt_task)
        worker.run()
        self.assertTrue(upt_task.complete())
        ht = hl.read_table(upt_task.output().path)
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    family_guids=['abc_1'],
                    family_samples={
                        'abc_1': ['HG00731_1', 'HG00732_1', 'HG00733_1'],
                    },
                    sample_type=SampleType.WGS.value,
                    updates={'v03_pipeline/var/test/callsets/1kg_30variants.vcf'},
                ),
            ],
        )

        self.assertCountEqual(
            ht.collect()[:2],
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=876499,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'G'],
                    filters=set(),
                    family_entries=[
                        [
                            hl.Struct(
                                GQ=21,
                                AB=1.0,
                                DP=7,
                                GT=hl.Call(alleles=[1, 1], phased=False),
                            ),
                            hl.Struct(
                                GQ=24,
                                AB=1.0,
                                DP=8,
                                GT=hl.Call(alleles=[1, 1], phased=False),
                            ),
                            hl.Struct(
                                GQ=12,
                                AB=1.0,
                                DP=4,
                                GT=hl.Call(alleles=[1, 1], phased=False),
                            ),
                        ],
                    ],
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=878314,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'C'],
                    filters=set(),
                    family_entries=[
                        [
                            hl.Struct(
                                GQ=30,
                                AB=hl.eval(hl.float32(0.3333333333333333)),
                                DP=3,
                                GT=hl.Call(alleles=[0, 1], phased=False),
                            ),
                            hl.Struct(
                                GQ=6,
                                AB=0.0,
                                DP=2,
                                GT=hl.Call(alleles=[0, 0], phased=False),
                            ),
                            hl.Struct(
                                GQ=61,
                                AB=hl.eval(hl.float32(0.6)),
                                DP=5,
                                GT=hl.Call(alleles=[0, 1], phased=False),
                            ),
                        ],
                    ],
                ),
            ],
        )
