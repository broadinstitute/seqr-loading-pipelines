import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.write_family_table import WriteFamilyTableTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_GCNV_BED_FILE = 'v03_pipeline/var/test/callsets/gcnv_1.tsv'
TEST_SNV_INDEL_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_SV_VCF = 'v03_pipeline/var/test/callsets/sv_1.vcf'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'
TEST_PEDIGREE_5 = 'v03_pipeline/var/test/pedigrees/test_pedigree_5.tsv'

TEST_RUN_ID = 'manual__2024-04-03'


class WriteFamilyTableTaskTest(MockedDatarootTestCase):
    def test_snv_write_family_table_task(self) -> None:
        worker = luigi.worker.Worker()
        wft_task = WriteFamilyTableTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id=TEST_RUN_ID,
            sample_type=SampleType.WGS,
            callset_path=TEST_SNV_INDEL_VCF,
            project_guids=['R0113_test_project'],
            project_remap_paths=[TEST_REMAP],
            project_pedigree_paths=[TEST_PEDIGREE_3],
            project_i=0,
            family_guid='abc_1',
            skip_validation=True,
        )
        worker.add(wft_task)
        worker.run()
        self.assertTrue(wft_task.complete())
        ht = hl.read_table(wft_task.output().path)
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    sample_ids=['HG00731_1', 'HG00732_1', 'HG00733_1'],
                    sample_type=SampleType.WGS.value,
                    updates={'v03_pipeline/var/test/callsets/1kg_30variants.vcf'},
                ),
            ],
        )
        self.assertEqual(
            ht.count(),
            16,
        )
        self.assertCountEqual(
            ht.entries.collect()[:5],
            [
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
                [
                    hl.Struct(
                        GQ=99,
                        AB=0.0,
                        DP=66,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                    hl.Struct(
                        GQ=99,
                        AB=hl.eval(hl.float32(0.5283018867924528)),
                        DP=53,
                        GT=hl.Call(alleles=[0, 1], phased=False),
                    ),
                    hl.Struct(
                        GQ=99,
                        AB=0.0,
                        DP=55,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                ],
                [
                    hl.Struct(
                        GQ=99,
                        AB=1.0,
                        DP=39,
                        GT=hl.Call(alleles=[1, 1], phased=False),
                    ),
                    hl.Struct(
                        GQ=99,
                        AB=0.0,
                        DP=61,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                    hl.Struct(
                        GQ=99,
                        AB=hl.eval(hl.float32(0.4146341463414634)),
                        DP=41,
                        GT=hl.Call(alleles=[0, 1], phased=False),
                    ),
                ],
                [
                    hl.Struct(
                        GQ=12,
                        AB=1.0,
                        DP=4,
                        GT=hl.Call(alleles=[1, 1], phased=False),
                    ),
                    hl.Struct(
                        GQ=9,
                        AB=1.0,
                        DP=3,
                        GT=hl.Call(alleles=[1, 1], phased=False),
                    ),
                    hl.Struct(
                        GQ=18,
                        AB=1.0,
                        DP=6,
                        GT=hl.Call(alleles=[1, 1], phased=False),
                    ),
                ],
            ],
        )

    def test_sv_write_family_table_task(self) -> None:
        worker = luigi.worker.Worker()
        write_family_table_task = WriteFamilyTableTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SV,
            run_id=TEST_RUN_ID,
            sample_type=SampleType.WGS,
            callset_path=TEST_SV_VCF,
            project_guids=['R0115_test_project2'],
            project_remap_paths=['not_a_real_file'],
            project_pedigree_paths=[TEST_PEDIGREE_5],
            project_i=0,
            family_guid='family_2_1',
            skip_validation=True,
        )
        worker.add(write_family_table_task)
        worker.run()
        self.assertTrue(write_family_table_task.complete())
        ht = hl.read_table(write_family_table_task.output().path)
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    updates={TEST_SV_VCF},
                    sample_type=SampleType.WGS.value,
                    sample_ids=[
                        'RGP_164_1',
                        'RGP_164_2',
                        'RGP_164_3',
                        'RGP_164_4',
                    ],
                ),
            ],
        )
        self.assertEqual(
            ht.count(),
            11,
        )
        self.assertCountEqual(
            ht.entries.collect()[:5],
            [
                [
                    hl.Struct(
                        CN=None,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=99,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                    hl.Struct(
                        CN=None,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=31,
                        GT=hl.Call(alleles=[0, 1], phased=False),
                    ),
                    hl.Struct(
                        CN=None,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=99,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                    hl.Struct(
                        CN=None,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=99,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                ],
                [
                    hl.Struct(
                        CN=None,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=59,
                        GT=hl.Call(alleles=[1, 1], phased=False),
                    ),
                    hl.Struct(
                        CN=None,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=26,
                        GT=hl.Call(alleles=[1, 1], phased=False),
                    ),
                    hl.Struct(
                        CN=None,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=39,
                        GT=hl.Call(alleles=[1, 1], phased=False),
                    ),
                    hl.Struct(
                        CN=None,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=19,
                        GT=hl.Call(alleles=[0, 1], phased=False),
                    ),
                ],
                [
                    hl.Struct(
                        CN=2,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=99,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                    hl.Struct(
                        CN=2,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=True,
                            new_call=False,
                        ),
                        GQ=57,
                        GT=hl.Call(alleles=[0, 1], phased=False),
                    ),
                    hl.Struct(
                        CN=2,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=0,
                        GT=hl.Call(alleles=[0, 1], phased=False),
                    ),
                    hl.Struct(
                        CN=3,
                        concordance=hl.Struct(
                            prev_num_alt=2,
                            prev_call=False,
                            new_call=False,
                        ),
                        GQ=99,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                ],
                [
                    hl.Struct(
                        CN=None,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=99,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                    hl.Struct(
                        CN=None,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=41,
                        GT=hl.Call(alleles=[0, 1], phased=False),
                    ),
                    hl.Struct(
                        CN=None,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=89,
                        GT=hl.Call(alleles=[1, 1], phased=False),
                    ),
                    hl.Struct(
                        CN=None,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=99,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                ],
                [
                    hl.Struct(
                        CN=None,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=52,
                        GT=hl.Call(alleles=[0, 1], phased=False),
                    ),
                    hl.Struct(
                        CN=None,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=99,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                    hl.Struct(
                        CN=None,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=99,
                        GT=hl.Call(alleles=[0, 0], phased=False),
                    ),
                    hl.Struct(
                        CN=None,
                        concordance=hl.Struct(
                            prev_num_alt=None,
                            prev_call=False,
                            new_call=True,
                        ),
                        GQ=62,
                        GT=hl.Call(alleles=[0, 1], phased=False),
                    ),
                ],
            ],
        )

    def test_gcnv_write_family_table_task(self) -> None:
        worker = luigi.worker.Worker()
        write_family_table_task = WriteFamilyTableTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.GCNV,
            run_id=TEST_RUN_ID,
            sample_type=SampleType.WES,
            callset_path=TEST_GCNV_BED_FILE,
            project_guids=['R0115_test_project2'],
            project_remap_paths=['not_a_real_file'],
            project_pedigree_paths=[TEST_PEDIGREE_5],
            project_i=0,
            family_guid='family_2_1',
            skip_validation=True,
        )
        worker.add(write_family_table_task)
        worker.run()
        self.assertTrue(write_family_table_task.complete())
        ht = hl.read_table(write_family_table_task.output().path)
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    updates={TEST_GCNV_BED_FILE},
                    sample_type=SampleType.WES.value,
                    sample_ids=[
                        'RGP_164_1',
                        'RGP_164_2',
                        'RGP_164_3',
                        'RGP_164_4',
                    ],
                ),
            ],
        )
        self.assertEqual(
            ht.count(),
            2,
        )
        self.assertCountEqual(
            ht.entries.collect(),
            [
                [
                    hl.Struct(
                        concordance=hl.Struct(
                            new_call=False,
                            prev_call=True,
                            prev_overlap=False,
                        ),
                        defragged=False,
                        sample_end=100007881,
                        sample_gene_ids={'ENSG00000283761', 'ENSG00000117620'},
                        sample_num_exon=2,
                        sample_start=100006937,
                        CN=1,
                        GT=hl.Call(alleles=[0, 1], phased=False),
                        QS=4,
                    ),
                    hl.Struct(
                        concordance=hl.Struct(
                            new_call=False,
                            prev_call=False,
                            prev_overlap=False,
                        ),
                        defragged=False,
                        sample_end=100023213,
                        sample_gene_ids={'ENSG00000283761', 'ENSG00000117620'},
                        sample_num_exon=None,
                        sample_start=100017585,
                        CN=1,
                        GT=hl.Call(alleles=[0, 1], phased=False),
                        QS=5,
                    ),
                    hl.Struct(
                        concordance=hl.Struct(
                            new_call=False,
                            prev_call=True,
                            prev_overlap=False,
                        ),
                        defragged=False,
                        sample_end=100023213,
                        sample_gene_ids={'ENSG00000283761', 'ENSG00000117620'},
                        sample_num_exon=None,
                        sample_start=100017585,
                        CN=0,
                        GT=hl.Call(alleles=[1, 1], phased=False),
                        QS=30,
                    ),
                    hl.Struct(
                        concordance=hl.Struct(
                            new_call=False,
                            prev_call=True,
                            prev_overlap=False,
                        ),
                        defragged=False,
                        sample_end=100023212,
                        sample_gene_ids={'ENSG00000283761', 'ENSG22222222222'},
                        sample_num_exon=2,
                        sample_start=100017586,
                        CN=0,
                        GT=hl.Call(alleles=[1, 1], phased=False),
                        QS=30,
                    ),
                ],
                [
                    hl.Struct(
                        concordance=None,
                        defragged=None,
                        sample_end=None,
                        sample_gene_ids=None,
                        sample_num_exon=None,
                        sample_start=None,
                        CN=None,
                        GT=None,
                        QS=None,
                    ),
                    hl.Struct(
                        concordance=None,
                        defragged=None,
                        sample_end=None,
                        sample_gene_ids=None,
                        sample_num_exon=None,
                        sample_start=None,
                        CN=None,
                        GT=None,
                        QS=None,
                    ),
                    hl.Struct(
                        concordance=hl.Struct(
                            new_call=False,
                            prev_call=True,
                            prev_overlap=False,
                        ),
                        defragged=False,
                        sample_end=None,
                        sample_gene_ids=None,
                        sample_num_exon=None,
                        sample_start=None,
                        CN=0,
                        GT=hl.Call(alleles=[1, 1], phased=False),
                        QS=30,
                    ),
                    hl.Struct(
                        concordance=None,
                        defragged=None,
                        sample_end=None,
                        sample_gene_ids=None,
                        sample_num_exon=None,
                        sample_start=None,
                        CN=None,
                        GT=None,
                        QS=None,
                    ),
                ],
            ],
        )
