import shutil

import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.update_project_table import UpdateProjectTableTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf.bgz'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'
TEST_SEX_CHECK_1 = 'v03_pipeline/var/test/sex_check/test_sex_check_1.ht'
TEST_RELATEDNESS_CHECK_1 = (
    'v03_pipeline/var/test/relatedness_check/test_relatedness_check_1.ht'
)


class UpdateProjectTableTaskTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        shutil.copytree(
            TEST_SEX_CHECK_1,
            f'{self.mock_env.LOADING_DATASETS}/v03/GRCh38/SNV_INDEL/sex_check/78d7998164bbe170d4f5282a66873df2e3b18099175069a32565fb0dc08dc3d4.ht',
        )
        shutil.copytree(
            TEST_RELATEDNESS_CHECK_1,
            f'{self.mock_env.LOADING_DATASETS}/v03/GRCh38/SNV_INDEL/relatedness_check/78d7998164bbe170d4f5282a66873df2e3b18099175069a32565fb0dc08dc3d4.ht',
        )

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
        self.assertEqual(
            upt_task.output().path,
            f'{self.mock_env.DATASETS}/v03/GRCh38/SNV_INDEL/projects/R0113_test_project.ht',
        )
        self.assertTrue(upt_task.complete())
        ht = hl.read_table(upt_task.output().path)
        self.assertCountEqual(
            ht.globals.sample_ids.collect(),
            [
                ['HG00731_1', 'HG00732_1', 'HG00733_1'],
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
                    entries=[
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
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=878314,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'C'],
                    filters=set(),
                    entries=[
                        hl.Struct(
                            GQ=30,
                            AB=0.3333333333333333,
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
                            AB=0.6,
                            DP=5,
                            GT=hl.Call(alleles=[0, 1], phased=False),
                        ),
                    ],
                ),
            ],
        )
