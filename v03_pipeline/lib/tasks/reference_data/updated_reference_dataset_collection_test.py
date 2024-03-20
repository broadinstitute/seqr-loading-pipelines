import shutil
from unittest import mock
from unittest.mock import ANY

import hail as hl
import luigi.worker

from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceDatasetCollection,
    ReferenceGenome,
    SampleType,
)
from v03_pipeline.lib.paths import valid_reference_dataset_collection_path
from v03_pipeline.lib.reference_data.config import CONFIG
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset_collection import (
    UpdatedReferenceDatasetCollectionTask,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

COMBINED_2_PATH = 'v03_pipeline/var/test/reference_data/test_combined_2.ht'

MOCK_PRIMATE_AI_DATASET_HT = hl.Table.parallelize(
    [
        {
            'locus': hl.Locus(
                contig='chr1',
                position=871269,
                reference_genome='GRCh38',
            ),
            'alleles': ['A', 'C'],
            'info': hl.Struct(score=0.25),
        },
    ],
    hl.tstruct(
        locus=hl.tlocus('GRCh38'),
        alleles=hl.tarray(hl.tstr),
        info=hl.tstruct(score=hl.tfloat32),
    ),
    key=['locus', 'alleles'],
    globals=hl.Struct(
        version='v0.3',
    ),
)
MOCK_CADD_DATASET_HT = hl.Table.parallelize(
    [
        {
            'locus': hl.Locus(
                contig='chr1',
                position=871269,
                reference_genome='GRCh38',
            ),
            'alleles': ['A', 'C'],
            'PHRED': 1,
        },
    ],
    hl.tstruct(
        locus=hl.tlocus('GRCh38'),
        alleles=hl.tarray(hl.tstr),
        PHRED=hl.tint32,
    ),
    key=['locus', 'alleles'],
    globals=hl.Struct(
        version='v1.6',
    ),
)
MOCK_CONFIG = {
    'primate_ai': {
        '38': {
            'version': 'v0.3',
            'source_path': 'gs://seqr-reference-data/GRCh38/primate_ai/PrimateAI_scores_v0.2.liftover_grch38.ht',
            'select': {
                'score': 'info.score',
            },
            'custom_import': lambda *_: MOCK_PRIMATE_AI_DATASET_HT,
        },
    },
    'cadd': {
        '38': {
            'version': 'v1.6',
            'source_path': 'gs://seqr-reference-data/GRCh38/CADD/CADD_snvs_and_indels.v1.6.ht',
            'select': ['PHRED'],
            'custom_import': lambda *_: MOCK_CADD_DATASET_HT,
        },
    },
    'clinvar': {
        '38': {
            **CONFIG['clinvar']['38'],
            'source_path': 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh37/clinvar.vcf.gz',
            'custom_import': lambda *_: hl.Table.parallelize(
                [
                    {
                        'locus': hl.Locus(
                            contig='chr1',
                            position=871269,
                            reference_genome='GRCh38',
                        ),
                        'alleles': ['A', 'C'],
                        'info': hl.Struct(
                            ALLELEID=1,
                            CLNSIG=[
                                'Pathogenic/Likely_pathogenic/Pathogenic',
                                '_low_penetrance',
                            ],
                            CLNSIGCONF=[
                                'Pathogenic(8)|Likely_pathogenic(2)|Pathogenic',
                                '_low_penetrance(1)|Uncertain_significance(1)',
                            ],
                            CLNREVSTAT=['no_classifications_from_unflagged_records'],
                        ),
                    },
                ],
                hl.tstruct(
                    locus=hl.tlocus('GRCh38'),
                    alleles=hl.tarray(hl.tstr),
                    info=hl.tstruct(
                        ALLELEID=hl.tint32,
                        CLNSIG=hl.tarray(hl.tstr),
                        CLNSIGCONF=hl.tarray(hl.tstr),
                        CLNREVSTAT=hl.tarray(hl.tstr),
                    ),
                ),
                key=['locus', 'alleles'],
                globals=hl.Struct(
                    version='2023-11-26',
                ),
            ),
        },
    },
}


class UpdatedReferenceDatasetCollectionTaskTest(MockedDatarootTestCase):

    @mock.patch.dict(
        'v03_pipeline.lib.reference_data.compare_globals.CONFIG',
        MOCK_CONFIG,
    )
    @mock.patch.dict(
        'v03_pipeline.lib.reference_data.dataset_table_operations.CONFIG',
        MOCK_CONFIG,
    )
    @mock.patch(
        'v03_pipeline.lib.reference_data.clinvar.download_and_import_clinvar_txt_file',
    )
    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_update_task_with_empty_reference_data_table(
        self,
        mock_rdc_datasets,
        mock_download_and_import_clinvar_txt_file,
    ) -> None:
        """
        Given a new task with no existing reference dataset collection table,
        expect the task to create a new reference dataset collection table for all datasets in the collection.
        """
        mock_rdc_datasets.return_value = ['cadd', 'primate_ai', 'clinvar']

        mock_download_and_import_clinvar_txt_file.side_effect = [
            hl.Table.parallelize(
                [],
                hl.tstruct(
                    *{'#VariationID': hl.tstr,
                      'ClinicalSignificance': hl.tstr,
                      'DateLastEvaluated': hl.tstr,
                      }
                ),
            )
            # table1 = submission summary
            # table2 = variant summary
        ]
        worker = luigi.worker.Worker()
        task = UpdatedReferenceDatasetCollectionTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            reference_dataset_collection=ReferenceDatasetCollection.COMBINED,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())

        ht = hl.read_table(task.output().path)
        print(ht.collect())
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'C'],
                    primate_ai=hl.Struct(score=0.25),
                    cadd=hl.Struct(PHRED=1),
                ),
            ],
        )
        self.assertEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    paths=hl.Struct(
                        primate_ai='gs://seqr-reference-data/GRCh38/primate_ai/PrimateAI_scores_v0.2.liftover_grch38.ht',
                        cadd='gs://seqr-reference-data/GRCh38/CADD/CADD_snvs_and_indels.v1.6.ht',
                    ),
                    versions=hl.Struct(primate_ai='v0.3', cadd='v1.6'),
                    enums=hl.Struct(
                        primate_ai=hl.Struct(),
                        cadd=hl.Struct(),
                    ),
                    date=ANY,
                ),
            ],
        )

    @mock.patch.dict(
        'v03_pipeline.lib.reference_data.compare_globals.CONFIG',
        MOCK_CONFIG,
    )
    @mock.patch.dict(
        'v03_pipeline.lib.reference_data.dataset_table_operations.CONFIG',
        MOCK_CONFIG,
    )
    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_update_task_with_existing_reference_dataset_collection_table(
        self,
        mock_rdc_datasets,
    ) -> None:
        """
        Given an existing reference dataset collection which contains only the primate_ai dataset and has globals:
            Struct(paths=Struct(primate_ai='gs://seqr-reference-data/GRCh38/primate_ai/PrimateAI_scores_v0.2.liftover_grch38.ht'),
                   versions=Struct(primate_ai='v0.2'),
                   enums=Struct(primate_ai=Struct()),
                   date=ANY),
        expect the task to update the existing reference dataset collection table with the new dataset (cadd),
        new values for primate_ai, and update the globals with the new primate_ai dataset's globals and cadd's globals.
        """
        # copy existing reference dataset collection (primate_ai only) in COMBINED_2_PATH to test path
        shutil.copytree(
            COMBINED_2_PATH,
            valid_reference_dataset_collection_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                ReferenceDatasetCollection.COMBINED,
            ),
        )

        mock_rdc_datasets.return_value = ['cadd', 'primate_ai']
        worker = luigi.worker.Worker()
        task = UpdatedReferenceDatasetCollectionTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            reference_dataset_collection=ReferenceDatasetCollection.COMBINED,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())

        ht = hl.read_table(task.output().path)
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'C'],
                    primate_ai=hl.Struct(
                        score=0.25,
                    ),  # expect row in primate_ai to be updated from 0.5 to 0.25
                    cadd=hl.Struct(PHRED=1),
                ),
            ],
        )
        self.assertEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    paths=hl.Struct(
                        cadd='gs://seqr-reference-data/GRCh38/CADD/CADD_snvs_and_indels.v1.6.ht',
                        primate_ai='gs://seqr-reference-data/GRCh38/primate_ai/PrimateAI_scores_v0.2.liftover_grch38.ht',
                    ),
                    versions=hl.Struct(
                        cadd='v1.6',
                        primate_ai='v0.3',  # expect primate_ai version to be updated
                    ),
                    enums=hl.Struct(
                        cadd=hl.Struct(),
                        primate_ai=hl.Struct(),
                    ),
                    date=ANY,
                ),
            ],
        )
