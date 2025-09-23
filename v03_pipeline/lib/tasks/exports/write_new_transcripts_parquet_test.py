import os

import hail as hl
import luigi.worker
import pandas as pd

from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
    SampleType,
)
from v03_pipeline.lib.paths import (
    new_transcripts_parquet_path,
    new_variants_table_path,
)
from v03_pipeline.lib.tasks.exports.write_new_transcripts_parquet import (
    WriteNewTranscriptsParquetTask,
)
from v03_pipeline.lib.test.misc import convert_ndarray_to_list
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_SNV_INDEL_ANNOTATIONS = (
    'v03_pipeline/var/test/exports/GRCh38/SNV_INDEL/annotations.ht'
)
TEST_GRCH37_SNV_INDEL_ANNOTATIONS = (
    'v03_pipeline/var/test/exports/GRCh37/SNV_INDEL/annotations.ht'
)

TEST_RUN_ID = 'manual__2024-04-03'


class WriteNewTranscriptsParquetTest(MockedDatarootTestCase):
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

        # Make an incomplete parquet to validate overwrite-ing.
        os.makedirs(
            new_transcripts_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
            exist_ok=True,
        )
        with open(
            os.path.join(
                new_transcripts_parquet_path(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    TEST_RUN_ID,
                ),
                'incomplete_file.parquet',
            ),
            'w',
        ) as f:
            f.write('')

    def test_write_new_transcripts_parquet_test(
        self,
    ) -> None:
        worker = luigi.worker.Worker()
        task = WriteNewTranscriptsParquetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path='fake_callset',
            project_guids=[
                'fake_project',
            ],
            skip_all_validations=True,
            run_id=TEST_RUN_ID,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        df = pd.read_parquet(
            new_transcripts_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        export_json = convert_ndarray_to_list(df.head(1).to_dict('records'))
        self.assertListEqual(list(export_json[0].keys()), ['key', 'transcripts'])
        self.assertEqual(
            export_json[0]['key'],
            0,
        )
        self.assertEqual(
            export_json[0]['transcripts'][0],
            {
                'alphamissense': {'pathogenicity': None},
                'aminoAcids': 'S/L',
                'biotype': 'protein_coding',
                'canonical': 1,
                'codons': 'tCg/tTg',
                'consequenceTerms': ['missense_variant'],
                'exon': {'index': 6, 'total': 14},
                'geneId': 'ENSG00000187634',
                'hgvsc': 'ENST00000616016.5:c.1049C>T',
                'hgvsp': 'ENSP00000478421.2:p.Ser350Leu',
                'intron': None,
                'transcriptId': 'ENST00000616016',
                'transcriptRank': 0,
                'majorConsequence': 'missense_variant',
                'maneSelect': 'NM_001385641.1',
                'manePlusClinical': None,
                'refseqTranscriptId': 'NM_001385641.1',
                'loftee': {'isLofNagnag': None, 'lofFilters': None},
                'spliceregion': {
                    'extended_intronic_splice_region_variant': False,
                },
                'utrannotator': {
                    'existingInframeOorfs': None,
                    'existingOutofframeOorfs': None,
                    'existingUorfs': None,
                    'fiveutrAnnotation': {
                        'AltStop': None,
                        'AltStopDistanceToCDS': None,
                        'CapDistanceToStart': None,
                        'DistanceToCDS': 41,
                        'DistanceToStop': None,
                        'Evidence': None,
                        'FrameWithCDS': None,
                        'KozakContext': 'CGCATGC',
                        'KozakStrength': 'Weak',
                        'StartDistanceToCDS': None,
                        'alt_type': None,
                        'alt_type_length': None,
                        'newSTOPDistanceToCDS': None,
                        'ref_StartDistanceToCDS': None,
                        'ref_type': None,
                        'ref_type_length': None,
                        'type': 'OutOfFrame_oORF',
                    },
                    'fiveutrConsequence': None,
                },
            },
        )
        self.assertEqual(
            list(export_json[0]['transcripts'][0].keys()),
            sorted(export_json[0]['transcripts'][0].keys()),
        )
        self.assertEqual(
            list(
                export_json[0]['transcripts'][0]['utrannotator'][
                    'fiveutrAnnotation'
                ].keys(),
            ),
            sorted(
                export_json[0]['transcripts'][0]['utrannotator'][
                    'fiveutrAnnotation'
                ].keys(),
            ),
        )

    def test_grch37_write_new_transcripts_parquet_test(
        self,
    ) -> None:
        worker = luigi.worker.Worker()
        task = WriteNewTranscriptsParquetTask(
            reference_genome=ReferenceGenome.GRCh37,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path='fake_callset',
            project_guids=[
                'fake_project',
            ],
            skip_all_validations=True,
            run_id=TEST_RUN_ID,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        df = pd.read_parquet(
            os.path.join(
                new_transcripts_parquet_path(
                    ReferenceGenome.GRCh37,
                    DatasetType.SNV_INDEL,
                    TEST_RUN_ID,
                ),
            ),
        )
        export_json = convert_ndarray_to_list(df.head(1).to_dict('records'))
        self.assertListEqual(list(export_json[0].keys()), ['key', 'transcripts'])
        self.assertEqual(
            export_json[0]['key'],
            1424,
        )
        self.assertEqual(
            export_json[0]['transcripts'][0],
            {
                'aminoAcids': 'E/G',
                'biotype': 'protein_coding',
                'canonical': 1,
                'codons': 'gAa/gGa',
                'consequenceTerms': ['missense_variant'],
                'geneId': 'ENSG00000186092',
                'hgvsc': 'ENST00000335137.3:c.44A>G',
                'hgvsp': 'ENSP00000334393.3:p.Glu15Gly',
                'loftee': {'isLofNagnag': None, 'lofFilters': None},
                'majorConsequence': 'missense_variant',
                'transcriptId': 'ENST00000335137',
                'transcriptRank': 0,
            },
        )
        self.assertEqual(
            list(export_json[0]['transcripts'][0].keys()),
            sorted(export_json[0]['transcripts'][0].keys()),
        )
