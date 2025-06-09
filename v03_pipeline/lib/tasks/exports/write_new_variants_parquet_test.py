from unittest import mock
from unittest.mock import Mock

import hail as hl
import luigi.worker
import pandas as pd

from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
    SampleType,
)
from v03_pipeline.lib.paths import (
    new_variants_parquet_path,
    new_variants_table_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.exports.write_new_variants_parquet import (
    WriteNewVariantsParquetTask,
)
from v03_pipeline.lib.test.misc import convert_ndarray_to_list
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_SNV_INDEL_ANNOTATIONS = (
    'v03_pipeline/var/test/exports/GRCh38/SNV_INDEL/annotations.ht'
)
TEST_GRCH37_SNV_INDEL_ANNOTATIONS = (
    'v03_pipeline/var/test/exports/GRCh37/SNV_INDEL/annotations.ht'
)
TEST_MITO_ANNOTATIONS = 'v03_pipeline/var/test/exports/GRCh38/MITO/annotations.ht'
TEST_SV_ANNOTATIONS = 'v03_pipeline/var/test/exports/GRCh38/SV/annotations.ht'
TEST_GCNV_ANNOTATIONS = 'v03_pipeline/var/test/exports/GRCh38/GCNV/annotations.ht'

TEST_RUN_ID = 'manual__2024-04-03'


class WriteNewVariantsParquetTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        ht = hl.read_table(
            TEST_SNV_INDEL_ANNOTATIONS,
        )
        ht.write(
            new_variants_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        ht = hl.read_table(
            TEST_GRCH37_SNV_INDEL_ANNOTATIONS,
        )
        ht.write(
            new_variants_table_path(
                ReferenceGenome.GRCh37,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        ht = hl.read_table(
            TEST_MITO_ANNOTATIONS,
        )
        ht.write(
            new_variants_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.MITO,
                TEST_RUN_ID,
            ),
        )
        ht = hl.read_table(
            TEST_SV_ANNOTATIONS,
        )
        ht.write(
            new_variants_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SV,
                TEST_RUN_ID,
            ),
        )
        ht = hl.read_table(TEST_GCNV_ANNOTATIONS)
        ht.write(
            variant_annotations_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.GCNV,
            ),
        )

    def test_write_new_variants_parquet_test(
        self,
    ) -> None:
        worker = luigi.worker.Worker()
        task = WriteNewVariantsParquetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path='fake_callset',
            project_guids=[
                'fake_project',
            ],
            project_pedigree_paths=['fake_pedigree'],
            skip_validation=True,
            run_id=TEST_RUN_ID,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        df = pd.read_parquet(
            new_variants_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        export_json = convert_ndarray_to_list(df.head(1).to_dict('records'))
        export_json[0]['sortedTranscriptConsequences'] = [
            export_json[0]['sortedTranscriptConsequences'][0],
        ]
        self.assertEqual(
            export_json,
            [
                {
                    'key': 0,
                    'xpos': 1000939121,
                    'chrom': '1',
                    'pos': 939121,
                    'ref': 'C',
                    'alt': 'T',
                    'variantId': '1-939121-C-T',
                    'rsid': None,
                    'CAID': 'CA502654',
                    'liftedOverChrom': '1',
                    'liftedOverPos': 874501,
                    'hgmd': {'accession': 'abcdefg', 'classification': 'DFP'},
                    'screenRegionType': None,
                    'predictions': {
                        'cadd': 23.5,
                        'eigen': 2.628000020980835,
                        'fathmm': 0.7174800038337708,
                        'gnomad_noncoding': None,
                        'mpc': 0.01291007362306118,
                        'mut_pred': None,
                        'mut_tester': 'D',
                        'polyphen': 0.164000004529953,
                        'primate_ai': 0.5918066501617432,
                        'revel': 0.3109999895095825,
                        'sift': 0.0010000000474974513,
                        'splice_ai': 0.0,
                        'splice_ai_consequence': 'No consequence',
                        'vest': 0.39500001072883606,
                    },
                    'populations': {
                        'exac': {
                            'ac': 20,
                            'af': 0.00019039999460801482,
                            'an': 47974,
                            'filter_af': 0.0007150234305299819,
                            'hemi': None,
                            'het': 20,
                            'hom': 0,
                        },
                        'gnomad_exomes': {
                            'ac': 964,
                            'af': 0.0006690866430290043,
                            'an': 1440770,
                            'filter_af': 0.0008023773552849889,
                            'hemi': 0,
                            'hom': 0,
                        },
                        'gnomad_genomes': {
                            'ac': 42,
                            'af': 0.0002759889466688037,
                            'an': 152180,
                            'filter_af': 0.0005293028079904616,
                            'hemi': 0,
                            'hom': 0,
                        },
                        'topmed': {
                            'ac': 41,
                            'af': 0.00032651599030941725,
                            'an': 125568,
                            'het': 41,
                            'hom': 0,
                        },
                    },
                    'sortedMotifFeatureConsequences': [
                        {
                            'consequenceTerms': ['TF_binding_site_variant'],
                            'motifFeatureId': 'ENSM00493959715',
                        },
                    ],
                    'sortedRegulatoryFeatureConsequences': [
                        {
                            'biotype': 'CTCF_binding_site',
                            'consequenceTerms': ['regulatory_region_variant'],
                            'regulatoryFeatureId': 'ENSR00000344437',
                        },
                    ],
                    'sortedTranscriptConsequences': [
                        {
                            'alphamissensePathogenicity': None,
                            'canonical': 1,
                            'consequenceTerms': ['missense_variant'],
                            'extendedIntronicSpliceRegionVariant': False,
                            'fiveutrConsequence': None,
                            'geneId': 'ENSG00000187634',
                        },
                    ],
                },
            ],
        )

    def test_grch37_write_new_variants_parquet_test(
        self,
    ) -> None:
        worker = luigi.worker.Worker()
        task = WriteNewVariantsParquetTask(
            reference_genome=ReferenceGenome.GRCh37,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path='fake_callset',
            project_guids=[
                'fake_project',
            ],
            project_pedigree_paths=['fake_pedigree'],
            skip_validation=True,
            run_id=TEST_RUN_ID,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        df = pd.read_parquet(
            new_variants_parquet_path(
                ReferenceGenome.GRCh37,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        export_json = convert_ndarray_to_list(df.head(1).to_dict('records'))
        export_json[0]['sortedTranscriptConsequences'] = [
            export_json[0]['sortedTranscriptConsequences'][0],
        ]
        self.assertEqual(
            export_json,
            [
                {
                    'key': 1424,
                    'xpos': 1000069134,
                    'chrom': '1',
                    'pos': 69134,
                    'ref': 'A',
                    'alt': 'G',
                    'variantId': '1-69134-A-G',
                    'rsid': None,
                    'CAID': 'CA502008',
                    'liftedOverChrom': '1',
                    'liftedOverPos': 69134,
                    'hgmd': None,
                    'predictions': {
                        'cadd': 15.880000114440918,
                        'eigen': 1.0019999742507935,
                        'fathmm': 0.056940000504255295,
                        'mpc': 1.8921889066696167,
                        'mut_pred': 0.3779999911785126,
                        'mut_tester': 'N',
                        'polyphen': 0.0010000000474974513,
                        'primate_ai': 0.37232041358947754,
                        'revel': 0.07500000298023224,
                        'sift': 0.1289999932050705,
                        'splice_ai': 0.019999999552965164,
                        'splice_ai_consequence': 'Donor gain',
                        'vest': 0.10700000077486038,
                    },
                    'populations': {
                        'exac': {
                            'ac': 0,
                            'af': 0.0016550000291317701,
                            'an': 66,
                            'filter_af': None,
                            'hemi': None,
                            'het': 0,
                            'hom': 0,
                        },
                        'gnomad_exomes': {
                            'ac': 505,
                            'af': 0.026665963232517242,
                            'an': 18938,
                            'filter_af': 0.08191808313131332,
                            'hemi': 0,
                            'hom': 127,
                        },
                        'gnomad_genomes': {
                            'ac': 1,
                            'af': 0.0001722949673421681,
                            'an': 5804,
                            'filter_af': 0.0005662514013238251,
                            'hemi': 0,
                            'hom': 0,
                        },
                        'topmed': {
                            'ac': 95,
                            'af': 0.0007565619889646769,
                            'an': 125568,
                            'het': 95,
                            'hom': 0,
                        },
                    },
                    'sortedTranscriptConsequences': [
                        {
                            'canonical': 1,
                            'consequenceTerms': ['missense_variant'],
                            'geneId': 'ENSG00000186092',
                        },
                    ],
                },
            ],
        )

    @mock.patch(
        'v03_pipeline.lib.tasks.exports.write_new_variants_parquet.WriteNewVariantsTableTask',
    )
    def test_mito_write_new_variants_parquet_test(
        self,
        write_new_variants_table_task: Mock,
    ) -> None:
        write_new_variants_table_task.return_value = MockCompleteTask()
        worker = luigi.worker.Worker()
        task = WriteNewVariantsParquetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.MITO,
            sample_type=SampleType.WGS,
            callset_path='fake_callset',
            project_guids=[
                'fake_project',
            ],
            project_pedigree_paths=['fake_pedigree'],
            skip_validation=True,
            run_id=TEST_RUN_ID,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        df = pd.read_parquet(
            new_variants_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.MITO,
                TEST_RUN_ID,
            ),
        )
        export_json = convert_ndarray_to_list(df.head(1).to_dict('records'))
        export_json[0]['sortedTranscriptConsequences'] = [
            export_json[0]['sortedTranscriptConsequences'][0],
        ]
        self.assertEqual(
            export_json,
            [
                {
                    'key': 998,
                    'xpos': 25000000578,
                    'chrom': 'M',
                    'pos': 578,
                    'ref': 'T',
                    'alt': 'C',
                    'variantId': 'M-578-T-C',
                    'rsid': 'rs1603218446',
                    'liftedOverChrom': 'M',
                    'liftedOverPos': 578,
                    'commonLowHeteroplasmy': True,
                    'mitomapPathogenic': None,
                    'predictions': {
                        'apogee': None,
                        'haplogroup_defining': None,
                        'hmtvar': 0.05000000074505806,
                        'mitotip': 'likely_pathogenic',
                        'mut_taster': None,
                        'sift': None,
                        'mlc': 0.12897999584674835,
                    },
                    'populations': {
                        'gnomad_mito': {'ac': 0, 'af': 0.0, 'an': 56433},
                        'gnomad_mito_heteroplasmy': {
                            'ac': 0,
                            'af': 0.0,
                            'an': 56433,
                            'max_hl': 0.0,
                        },
                        'helix': {'ac': None, 'af': None, 'an': None},
                        'helix_heteroplasmy': {
                            'ac': None,
                            'af': None,
                            'an': None,
                            'max_hl': None,
                        },
                    },
                    'sortedTranscriptConsequences': [
                        {
                            'canonical': 1,
                            'consequenceTerms': ['non_coding_transcript_exon_variant'],
                            'geneId': 'ENSG00000210049',
                        },
                    ],
                },
            ],
        )

    @mock.patch(
        'v03_pipeline.lib.tasks.exports.write_new_variants_parquet.WriteNewVariantsTableTask',
    )
    def test_sv_write_new_variants_parquet_test(
        self,
        write_new_variants_table_task: Mock,
    ) -> None:
        write_new_variants_table_task.return_value = MockCompleteTask()
        worker = luigi.worker.Worker()
        task = WriteNewVariantsParquetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SV,
            sample_type=SampleType.WGS,
            callset_path='fake_callset',
            project_guids=[
                'fake_project',
            ],
            project_pedigree_paths=['fake_pedigree'],
            skip_validation=True,
            run_id=TEST_RUN_ID,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        df = pd.read_parquet(
            new_variants_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SV,
                TEST_RUN_ID,
            ),
        )
        export_json = convert_ndarray_to_list(df.head(1).to_dict('records'))
        export_json[0]['sortedGeneConsequences'] = [
            export_json[0]['sortedGeneConsequences'][0],
        ]
        self.assertEqual(
            export_json,
            [
                {
                    'key': 727,
                    'xpos': 1001025886,
                    'chrom': '1',
                    'pos': 1025886,
                    'end': 1028192,
                    'rg37LocusEnd': {'contig': '1', 'position': 963572},
                    'variantId': 'all_sample_sets.chr1.final_cleanup_CPX_chr1_1',
                    'liftedOverChrom': '1',
                    'liftedOverPos': 961266,
                    'algorithms': 'manta',
                    'bothsidesSupport': None,
                    'cpxIntervals': [
                        {'chrom': '1', 'start': 1025886, 'end': 1025986, 'type': 'DUP'},
                        {'chrom': '1', 'start': 1025886, 'end': 1028192, 'type': 'INV'},
                    ],
                    'endChrom': '2',
                    'svSourceDetail': None,
                    'svType': 'CPX',
                    'svTypeDetail': 'dupINV',
                    'predictions': {'strvctvre': None},
                    'populations': {'gnomad_svs': None},
                    'sortedGeneConsequences': [
                        {'geneId': 'ENSG00000188157', 'majorConsequence': 'INTRONIC'},
                    ],
                },
            ],
        )

    @mock.patch(
        'v03_pipeline.lib.tasks.exports.write_new_variants_parquet.UpdateVariantAnnotationsTableWithNewSamplesTask',
    )
    def test_gcnv_write_new_variants_parquet_test(
        self,
        update_variant_annotations_task: Mock,
    ) -> None:
        update_variant_annotations_task.return_value = MockCompleteTask()
        worker = luigi.worker.Worker()
        task = WriteNewVariantsParquetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.GCNV,
            sample_type=SampleType.WES,
            callset_path='fake_callset',
            project_guids=[
                'fake_project',
            ],
            project_pedigree_paths=['fake_pedigree'],
            skip_validation=True,
            run_id=TEST_RUN_ID,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        df = pd.read_parquet(
            new_variants_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.GCNV,
                TEST_RUN_ID,
            ),
        )
        export_json = convert_ndarray_to_list(df.head(1).to_dict('records'))
        self.assertEqual(
            export_json,
            [
                {
                    'key': 0,
                    'xpos': 1000939203,
                    'chrom': '1',
                    'pos': 939203,
                    'end': 939558,
                    'rg37LocusEnd': {'contig': '1', 'position': 874938},
                    'variantId': 'R4_variant_0_DUP',
                    'liftedOverChrom': '1',
                    'liftedOverPos': 874583,
                    'numExon': 1,
                    'svType': 'DUP',
                    'predictions': {'strvctvre': 0.4490000009536743},
                    'populations': {
                        'seqrPop': {
                            'af': 4.3387713958509266e-05,
                            'ac': 1,
                            'an': 23048,
                            'Hom': None,
                            'Het': None,
                        },
                    },
                    'sortedGeneConsequences': [
                        {'geneId': 'ENSG00000187634', 'majorConsequence': 'LOF'},
                    ],
                },
            ],
        )
