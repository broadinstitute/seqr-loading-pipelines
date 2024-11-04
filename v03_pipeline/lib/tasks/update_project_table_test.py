import hail as hl
import luigi.worker

from v03_pipeline.lib.misc.io import remap_pedigree_hash
from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.update_project_table import UpdateProjectTableTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'
TEST_PEDIGREE_3_DIFFERENT_FAMILIES = (
    'v03_pipeline/var/test/pedigrees/test_pedigree_3_different_families.tsv'
)

TEST_RUN_ID = 'manual__2024-04-03'


class UpdateProjectTableTaskTest(MockedDatarootTestCase):
    def test_update_project_table_task(self) -> None:
        worker = luigi.worker.Worker()
        upt_task = UpdateProjectTableTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id=TEST_RUN_ID,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_guids=['R0113_test_project'],
            project_remap_paths=[TEST_REMAP],
            project_pedigree_paths=[TEST_PEDIGREE_3],
            project_i=0,
            skip_validation=True,
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
                    updates={
                        hl.Struct(
                            callset='v03_pipeline/var/test/callsets/1kg_30variants.vcf',
                            remap_pedigree_hash=hl.eval(
                                remap_pedigree_hash(
                                    TEST_REMAP,
                                    TEST_PEDIGREE_3,
                                ),
                            ),
                        ),
                    },
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

    def test_update_project_table_task_different_pedigree(self) -> None:
        worker = luigi.worker.Worker()
        upt_task = UpdateProjectTableTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id=TEST_RUN_ID,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_guids=['R0113_test_project'],
            project_remap_paths=[TEST_REMAP],
            project_pedigree_paths=[TEST_PEDIGREE_3],
            project_i=0,
            skip_validation=True,
        )
        worker.add(upt_task)
        worker.run()
        upt_task = UpdateProjectTableTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id=TEST_RUN_ID,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_guids=['R0113_test_project'],
            project_remap_paths=[TEST_REMAP],
            project_pedigree_paths=[TEST_PEDIGREE_3_DIFFERENT_FAMILIES],
            project_i=0,
            skip_validation=True,
        )
        worker.add(upt_task)
        worker.run()
        self.assertTrue(upt_task.complete())
        worker.add(upt_task)
        worker.run()
        ht = hl.read_table(upt_task.output().path)
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    family_guids=['abc_1'],
                    family_samples={
                        'abc_1': ['HG00731_1', 'HG00733_1'],
                    },
                    sample_type=SampleType.WGS.value,
                    updates={
                        hl.Struct(
                            callset='v03_pipeline/var/test/callsets/1kg_30variants.vcf',
                            remap_pedigree_hash=hl.eval(
                                remap_pedigree_hash(
                                    TEST_REMAP,
                                    TEST_PEDIGREE_3,
                                ),
                            ),
                        ),
                        hl.Struct(
                            callset='v03_pipeline/var/test/callsets/1kg_30variants.vcf',
                            remap_pedigree_hash=hl.eval(
                                remap_pedigree_hash(
                                    TEST_REMAP,
                                    TEST_PEDIGREE_3_DIFFERENT_FAMILIES,
                                ),
                            ),
                        ),
                    },
                ),
            ],
        )

        self.assertCountEqual(
            ht.collect()[0],
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
                            GQ=12,
                            AB=1.0,
                            DP=4,
                            GT=hl.Call(alleles=[1, 1], phased=False),
                        ),
                    ],
                ],
            ),
        )
