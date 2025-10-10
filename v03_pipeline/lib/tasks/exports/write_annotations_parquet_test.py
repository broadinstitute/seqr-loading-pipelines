import hail as hl
import luigi.worker
import pandas as pd

from v03_pipeline.lib.misc.io import remap_pedigree_hash
from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
    SampleType,
)
from v03_pipeline.lib.paths import (
    project_pedigree_path,
    variant_annotations_parquet_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.exports.write_annotations_parquet import (
    WriteVariantAnnotationsParquetTask,
)
from v03_pipeline.lib.test.misc import (
    convert_ndarray_to_list,
    copy_project_pedigree_to_mocked_dir,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_SNV_INDEL_ANNOTATIONS = (
    'v03_pipeline/var/test/exports/GRCh38/SNV_INDEL/annotations.ht'
)
TEST_PEDIGREE_3_REMAP = 'v03_pipeline/var/test/pedigrees/test_pedigree_3_remap.tsv'

TEST_RUN_ID = 'manual__2024-04-03'


class WriteAnnotationsParquetTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        copy_project_pedigree_to_mocked_dir(
            TEST_PEDIGREE_3_REMAP,
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            SampleType.WGS,
            'R0113_test_project',
        )
        super().setUp()
        ht = hl.read_table(
            TEST_SNV_INDEL_ANNOTATIONS,
        )
        ht = ht.annotate_globals(
            updates=ht.updates.add(
                hl.Struct(
                    callset='fake_callset',
                    project_guid='R0113_test_project',
                    remap_pedigree_hash=remap_pedigree_hash(
                        project_pedigree_path(
                            ReferenceGenome.GRCh38,
                            DatasetType.SNV_INDEL,
                            SampleType.WGS,
                            'R0113_test_project',
                        ),
                    ),
                ),
            ),
        )
        ht.write(
            variant_annotations_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
            ),
        )

    def test_write_annotations_parquet_test(
        self,
    ) -> None:
        worker = luigi.worker.Worker()
        task = WriteVariantAnnotationsParquetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path='fake_callset',
            project_guids=[
                'R0113_test_project',
            ],
            skip_validation=True,
            run_id=TEST_RUN_ID,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        df = pd.read_parquet(
            variant_annotations_parquet_path(
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
                    'xpos': 1000876499,
                    'chrom': '1',
                    'pos': 876499,
                    'ref': 'A',
                    'alt': 'G',
                    'variantId': '1-876499-A-G',
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
                            'filter_af': 0.10000000149011612,
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
