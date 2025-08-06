import hail as hl
import luigi.worker
import pandas as pd

from v03_pipeline.lib.annotations import shared
from v03_pipeline.lib.misc.io import import_callset, remap_pedigree_hash
from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
    SampleType,
)
from v03_pipeline.lib.paths import (
    new_entries_parquet_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.exports.write_new_entries_parquet import (
    WriteNewEntriesParquetTask,
)
from v03_pipeline.lib.test.misc import convert_ndarray_to_list
from v03_pipeline.lib.test.mocked_reference_datasets_testcase import (
    MockedReferenceDatasetsTestCase,
)

TEST_PEDIGREE_3_REMAP = 'v03_pipeline/var/test/pedigrees/test_pedigree_3_remap.tsv'
TEST_PEDIGREE_4_REMAP = 'v03_pipeline/var/test/pedigrees/test_pedigree_4_remap.tsv'
TEST_PEDIGREE_5 = 'v03_pipeline/var/test/pedigrees/test_pedigree_5.tsv'
TEST_MITO_EXPORT_PEDIGREE = (
    'v03_pipeline/var/test/pedigrees/test_mito_export_pedigree.tsv'
)
TEST_PEDIGREE_5 = 'v03_pipeline/var/test/pedigrees/test_pedigree_5.tsv'
TEST_SNV_INDEL_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_MITO_CALLSET = 'v03_pipeline/var/test/callsets/mito_1.mt'
TEST_SV_VCF_2 = 'v03_pipeline/var/test/callsets/sv_2.vcf'
TEST_GCNV_BED_FILE = 'v03_pipeline/var/test/callsets/gcnv_1.tsv'


HIGH_GNOMAD_AF_VARIANT_KEY = 2
TEST_RUN_ID = 'manual__2024-04-03'


class WriteNewEntriesParquetTest(MockedReferenceDatasetsTestCase):
    def setUp(self) -> None:
        # NOTE: The annotations tables are mocked for SNV_INDEL & MITO
        # to avoid reference dataset updates that SV/GCNV do not have.
        super().setUp()
        mt = import_callset(
            TEST_SNV_INDEL_VCF,
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
        )
        ht = mt.rows()
        ht = ht.add_index(name='key_')
        ht = ht.annotate(xpos=shared.xpos(ht))
        ht = ht.annotate(
            gnomad_genomes=hl.Struct(
                AF_POPMAX_OR_GLOBAL=hl.if_else(
                    ht.key_ == HIGH_GNOMAD_AF_VARIANT_KEY, 0.1, 0.005,
                ),
            ),
        )
        ht = ht.annotate_globals(
            updates={
                hl.Struct(
                    callset=TEST_SNV_INDEL_VCF,
                    project_guid='R0113_test_project',
                    remap_pedigree_hash=remap_pedigree_hash(TEST_PEDIGREE_3_REMAP),
                ),
                hl.Struct(
                    callset=TEST_SNV_INDEL_VCF,
                    project_guid='R0114_project4',
                    remap_pedigree_hash=remap_pedigree_hash(TEST_PEDIGREE_4_REMAP),
                ),
            },
        )
        ht.write(
            variant_annotations_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
            ),
        )
        mt = import_callset(TEST_MITO_CALLSET, ReferenceGenome.GRCh38, DatasetType.MITO)
        ht = mt.rows()
        ht = ht.add_index(name='key_')
        ht = ht.annotate(xpos=shared.xpos(ht))
        ht = ht.annotate_globals(
            updates={
                hl.Struct(
                    callset=TEST_MITO_CALLSET,
                    project_guid='R0116_test_project3',
                    remap_pedigree_hash=remap_pedigree_hash(TEST_MITO_EXPORT_PEDIGREE),
                ),
            },
        )
        ht.write(
            variant_annotations_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.MITO,
            ),
        )

    def test_write_new_entries_parquet(self):
        worker = luigi.worker.Worker()
        task = WriteNewEntriesParquetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path=TEST_SNV_INDEL_VCF,
            project_guids=['R0113_test_project', 'R0114_project4'],
            project_pedigree_paths=[TEST_PEDIGREE_3_REMAP, TEST_PEDIGREE_4_REMAP],
            skip_validation=True,
            run_id=TEST_RUN_ID,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        df = pd.read_parquet(
            new_entries_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        export_json = convert_ndarray_to_list(df.to_dict('records'))
        self.assertEqual(
            export_json[:2],
            [
                {
                    'key': HIGH_GNOMAD_AF_VARIANT_KEY,
                    'project_guid': 'R0113_test_project',
                    'family_guid': 'abc_1',
                    'sample_type': 'WGS',
                    'xpos': 1000876499,
                    'is_gnomad_gt_5_percent': True,
                    'filters': [],
                    'calls': [
                        {
                            'sampleId': 'HG00731_1',
                            'gt': 2,
                            'gq': 21,
                            'ab': 1.0,
                            'dp': 7,
                        },
                        {
                            'sampleId': 'HG00732_1',
                            'gt': 2,
                            'gq': 24,
                            'ab': 1.0,
                            'dp': 8,
                        },
                        {
                            'sampleId': 'HG00733_1',
                            'gt': 2,
                            'gq': 12,
                            'ab': 1.0,
                            'dp': 4,
                        },
                    ],
                    'sign': 1,
                },
                {
                    'key': 3,
                    'project_guid': 'R0113_test_project',
                    'family_guid': 'abc_1',
                    'sample_type': 'WGS',
                    'xpos': 1000878314,
                    'is_gnomad_gt_5_percent': False,
                    'filters': ['VQSRTrancheSNP99.00to99.90'],
                    'calls': [
                        {
                            'sampleId': 'HG00731_1',
                            'gt': 1,
                            'gq': 30,
                            'ab': 0.3333333432674408,
                            'dp': 3,
                        },
                        {'sampleId': 'HG00732_1', 'gt': 0, 'gq': 6, 'ab': 0.0, 'dp': 2},
                        {
                            'sampleId': 'HG00733_1',
                            'gt': 1,
                            'gq': 61,
                            'ab': 0.6000000238418579,
                            'dp': 5,
                        },
                    ],
                    'sign': 1,
                },
            ],
        )
        self.assertEqual(
            export_json[-1],
            {
                'key': 27,
                'family_guid': 'def_1',
                'filters': [],
                'is_gnomad_gt_5_percent': False,
                'project_guid': 'R0114_project4',
                'sample_type': 'WGS',
                'xpos': 1000902024,
                'calls': [
                    {
                        'sampleId': 'NA20885_1',
                        'gt': 1,
                        'gq': 4,
                        'ab': 0.10000000149011612,
                        'dp': 10,
                    },
                ],
                'sign': 1,
            },
        )

    def test_mito_write_new_entries_parquet(self):
        worker = luigi.worker.Worker()
        task = WriteNewEntriesParquetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.MITO,
            sample_type=SampleType.WGS,
            callset_path=TEST_MITO_CALLSET,
            project_guids=['R0116_test_project3'],
            project_pedigree_paths=[TEST_MITO_EXPORT_PEDIGREE],
            skip_validation=True,
            run_id=TEST_RUN_ID,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        df = pd.read_parquet(
            new_entries_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.MITO,
                TEST_RUN_ID,
            ),
        )
        export_json = convert_ndarray_to_list(df.to_dict('records'))
        self.assertEqual(
            export_json,
            [
                {
                    'key': 1,
                    'project_guid': 'R0116_test_project3',
                    'family_guid': 'family_1',
                    'sample_type': 'WGS',
                    'xpos': 25000000008,
                    'filters': [],
                    'calls': [
                        {
                            'sampleId': 'RGP_1270_2',
                            'gt': 2,
                            'dp': 4216,
                            'hl': 0.999,
                            'mitoCn': 224,
                            'contamination': 0.0,
                        },
                    ],
                    'sign': 1,
                },
                {
                    'key': 2,
                    'project_guid': 'R0116_test_project3',
                    'family_guid': 'family_1',
                    'sample_type': 'WGS',
                    'xpos': 25000000012,
                    'filters': [],
                    'calls': [
                        {
                            'sampleId': 'RGP_1270_2',
                            'gt': 2,
                            'dp': 4336,
                            'hl': 1.0,
                            'mitoCn': 224,
                            'contamination': 0.0,
                        },
                    ],
                    'sign': 1,
                },
                {
                    'key': 4,
                    'project_guid': 'R0116_test_project3',
                    'family_guid': 'family_1',
                    'sample_type': 'WGS',
                    'xpos': 25000000018,
                    'filters': [],
                    'calls': [
                        {
                            'sampleId': 'RGP_1270_2',
                            'gt': 2,
                            'dp': 4319,
                            'hl': 1.0,
                            'mitoCn': 224,
                            'contamination': 0.0,
                        },
                    ],
                    'sign': 1,
                },
            ],
        )

    def test_sv_write_new_entries_parquet(self):
        worker = luigi.worker.Worker()
        task = WriteNewEntriesParquetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SV,
            sample_type=SampleType.WGS,
            callset_path=TEST_SV_VCF_2,
            project_guids=['R0115_test_project2'],
            project_pedigree_paths=[TEST_PEDIGREE_5],
            skip_validation=True,
            run_id=TEST_RUN_ID,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        df = pd.read_parquet(
            new_entries_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SV,
                TEST_RUN_ID,
            ),
        )
        export_json = convert_ndarray_to_list(df.to_dict('records'))
        self.assertEqual(
            export_json,
            [
                {
                    'key': 0,
                    'project_guid': 'R0115_test_project2',
                    'family_guid': 'family_2_1',
                    'xpos': 1000180929,
                    'filters': ['HIGH_SR_BACKGROUND', 'UNRESOLVED'],
                    'calls': [
                        {
                            'sampleId': 'RGP_164_1',
                            'gt': 0,
                            'cn': None,
                            'gq': 99,
                            'newCall': True,
                            'prevCall': False,
                            'prevNumAlt': None,
                        },
                        {
                            'sampleId': 'RGP_164_2',
                            'gt': 1,
                            'cn': None,
                            'gq': 31,
                            'newCall': True,
                            'prevCall': False,
                            'prevNumAlt': None,
                        },
                        {
                            'sampleId': 'RGP_164_3',
                            'gt': 0,
                            'cn': None,
                            'gq': 99,
                            'newCall': True,
                            'prevCall': False,
                            'prevNumAlt': None,
                        },
                        {
                            'sampleId': 'RGP_164_4',
                            'gt': 0,
                            'cn': None,
                            'gq': 99,
                            'newCall': True,
                            'prevCall': False,
                            'prevNumAlt': None,
                        },
                    ],
                    'sign': 1,
                },
                {
                    'key': 1,
                    'project_guid': 'R0115_test_project2',
                    'family_guid': 'family_2_1',
                    'xpos': 1000257667,
                    'filters': [],
                    'calls': [
                        {
                            'sampleId': 'RGP_164_1',
                            'gt': 0,
                            'cn': 2.0,
                            'gq': 99,
                            'newCall': True,
                            'prevCall': False,
                            'prevNumAlt': None,
                        },
                        {
                            'sampleId': 'RGP_164_2',
                            'gt': 0,
                            'cn': 2.0,
                            'gq': 99,
                            'newCall': True,
                            'prevCall': False,
                            'prevNumAlt': None,
                        },
                        {
                            'sampleId': 'RGP_164_3',
                            'gt': 1,
                            'cn': 3.0,
                            'gq': 8,
                            'newCall': True,
                            'prevCall': False,
                            'prevNumAlt': None,
                        },
                        {
                            'sampleId': 'RGP_164_4',
                            'gt': 0,
                            'cn': 1.0,
                            'gq': 13,
                            'newCall': True,
                            'prevCall': False,
                            'prevNumAlt': None,
                        },
                    ],
                    'sign': 1,
                },
            ],
        )

    def test_gcnv_write_new_entries_parquet(self):
        worker = luigi.worker.Worker()
        task = WriteNewEntriesParquetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.GCNV,
            sample_type=SampleType.WES,
            callset_path=TEST_GCNV_BED_FILE,
            project_guids=['R0115_test_project2'],
            project_pedigree_paths=[TEST_PEDIGREE_5],
            skip_validation=True,
            run_id=TEST_RUN_ID,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        df = pd.read_parquet(
            new_entries_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.GCNV,
                TEST_RUN_ID,
            ),
        )
        export_json = convert_ndarray_to_list(df.to_dict('records'))
        self.assertEqual(
            export_json,
            [
                {
                    'key': 0,
                    'project_guid': 'R0115_test_project2',
                    'family_guid': 'family_2_1',
                    'xpos': 1100006937,
                    'filters': [],
                    'calls': [
                        {
                            'sampleId': 'RGP_164_1',
                            'gt': 1,
                            'cn': 1,
                            'qs': 4,
                            'defragged': False,
                            'start': 100006937,
                            'end': 100007881,
                            'numExon': 2,
                            'geneIds': ['ENSG00000117620', 'ENSG00000283761'],
                            'newCall': False,
                            'prevCall': True,
                            'prevOverlap': False,
                        },
                        {
                            'sampleId': 'RGP_164_2',
                            'gt': 1,
                            'cn': 1,
                            'qs': 5,
                            'defragged': False,
                            'start': 100017585,
                            'end': 100023213,
                            'numExon': 3,
                            'geneIds': ['ENSG00000117620', 'ENSG00000283761'],
                            'newCall': False,
                            'prevCall': False,
                            'prevOverlap': False,
                        },
                        {
                            'sampleId': 'RGP_164_3',
                            'gt': 2,
                            'cn': 0,
                            'qs': 30,
                            'defragged': False,
                            'start': 100017585,
                            'end': 100023213,
                            'numExon': 3,
                            'geneIds': ['ENSG00000117620', 'ENSG00000283761'],
                            'newCall': False,
                            'prevCall': True,
                            'prevOverlap': False,
                        },
                        {
                            'sampleId': 'RGP_164_4',
                            'gt': 2,
                            'cn': 0,
                            'qs': 30,
                            'defragged': False,
                            'start': 100017586,
                            'end': 100023212,
                            'numExon': 2,
                            'geneIds': ['ENSG00000283761', 'ENSG22222222222'],
                            'newCall': False,
                            'prevCall': True,
                            'prevOverlap': False,
                        },
                    ],
                    'sign': 1,
                },
                {
                    'key': 1,
                    'project_guid': 'R0115_test_project2',
                    'family_guid': 'family_2_1',
                    'xpos': 1100017586,
                    'filters': [],
                    'calls': [
                        {
                            'sampleId': 'RGP_164_1',
                            'gt': None,
                            'cn': None,
                            'qs': None,
                            'defragged': None,
                            'start': 100017586,
                            'end': 100023212,
                            'numExon': 2,
                            'geneIds': ['ENSG00000283761', 'ENSG22222222222'],
                            'newCall': None,
                            'prevCall': None,
                            'prevOverlap': None,
                        },
                        {
                            'sampleId': 'RGP_164_2',
                            'gt': None,
                            'cn': None,
                            'qs': None,
                            'defragged': None,
                            'start': 100017586,
                            'end': 100023212,
                            'numExon': 2,
                            'geneIds': ['ENSG00000283761', 'ENSG22222222222'],
                            'newCall': None,
                            'prevCall': None,
                            'prevOverlap': None,
                        },
                        {
                            'sampleId': 'RGP_164_3',
                            'gt': 2,
                            'cn': 0.0,
                            'qs': 30.0,
                            'defragged': False,
                            'start': 100017586,
                            'end': 100023212,
                            'numExon': 2,
                            'geneIds': ['ENSG00000283761', 'ENSG22222222222'],
                            'newCall': False,
                            'prevCall': True,
                            'prevOverlap': False,
                        },
                        {
                            'sampleId': 'RGP_164_4',
                            'gt': None,
                            'cn': None,
                            'qs': None,
                            'defragged': None,
                            'start': 100017586,
                            'end': 100023212,
                            'numExon': 2,
                            'geneIds': ['ENSG00000283761', 'ENSG22222222222'],
                            'newCall': None,
                            'prevCall': None,
                            'prevOverlap': None,
                        },
                    ],
                    'sign': 1,
                },
            ],
        )
