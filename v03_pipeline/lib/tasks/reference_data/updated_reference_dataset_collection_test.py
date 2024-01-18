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
            'primate_ai': hl.Struct(score=0.25),
        },
    ],
    hl.tstruct(
        locus=hl.tlocus('GRCh38'),
        alleles=hl.tarray(hl.tstr),
        primate_ai=hl.tstruct(score=hl.tfloat32),
    ),
    key=['locus', 'alleles'],
    globals=hl.Struct(
        path='gs://seqr-reference-data/GRCh38/primate_ai/PrimateAI_scores_v0.2.liftover_grch38.ht',
        version='v0.3',
        enums=hl.Struct(new_enum=['A', 'B']),
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
            'cadd': 1,
        },
    ],
    hl.tstruct(
        locus=hl.tlocus('GRCh38'),
        alleles=hl.tarray(hl.tstr),
        cadd=hl.tint32,
    ),
    key=['locus', 'alleles'],
    globals=hl.Struct(
        path='gs://seqr-reference-data/GRCh38/CADD/CADD_snvs_and_indels.v1.6.ht',
        version='v1.6',
        enums=hl.Struct(assertion=['A', 'B']),
    ),
)


class UpdatedReferenceDatasetCollectionTaskTest(MockedDatarootTestCase):
    @mock.patch(
        'v03_pipeline.lib.reference_data.dataset_table_operations.get_dataset_ht',
    )
    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_update_task_with_dataset_param_and_empty_reference_data_table(
        self,
        mock_reference_dataset_collection_datasets,
        mock_get_dataset_ht,
    ) -> None:
        """
        Given a new task with a dataset parameter and no existing reference dataset collection table,
        expect the task to create a new reference dataset collection table for all datasets in the collection and
        to ignore the given dataset parameter.
        """
        mock_reference_dataset_collection_datasets.return_value = ['primate_ai', 'cadd']

        # mock tables for both datasets
        mock_get_dataset_ht.side_effect = [
            MOCK_PRIMATE_AI_DATASET_HT,
            MOCK_CADD_DATASET_HT,
        ]

        worker = luigi.worker.Worker()
        # create task with no dataset parameter
        task = UpdatedReferenceDatasetCollectionTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            reference_dataset_collection=ReferenceDatasetCollection.COMBINED,
            dataset='primate_ai',
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
                    primate_ai=hl.Struct(score=0.25),
                    cadd=1,
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
                        primate_ai=hl.Struct(new_enum=['A', 'B']),
                        cadd=hl.Struct(assertion=['A', 'B']),
                    ),
                    date=ANY,
                ),
            ],
        )

    @mock.patch.dict(
        'v03_pipeline.lib.reference_data.dataset_table_operations.CONFIG',
        {
            'primate_ai': {
                '38': {
                    'path': 'gs://seqr-reference-data/GRCh38/primate_ai/PrimateAI_scores_v0.2.liftover_grch38.ht',
                    'version': 'v0.3',
                    'select': ['score'],
                    'enum_select': {
                        'new_enum': ['A', 'B'],
                    },
                },
            },
        },
    )
    @mock.patch(
        'v03_pipeline.lib.reference_data.dataset_table_operations.get_dataset_ht',
    )
    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_update_task_with_existing_reference_dataset_collection_table(
        self,
        mock_reference_dataset_collection_datasets,
        mock_get_dataset_ht,
    ) -> None:
        """
        Given an existing reference dataset collection which contains only the primate_ai dataset and has globals:
            Struct(paths=Struct(primate_ai='gs://seqr-reference-data/GRCh38/primate_ai/PrimateAI_scores_v0.2.liftover_grch38.ht'),
                   versions=Struct(primate_ai='v0.2'),
                   enums=Struct(primate_ai=Struct()),
                   date=ANY)
        , a new task with a dataset parameter, and 1 other dataset for the collection (cadd),
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

        mock_reference_dataset_collection_datasets.return_value = ['primate_ai', 'cadd']
        mock_get_dataset_ht.side_effect = [
            MOCK_PRIMATE_AI_DATASET_HT,
            MOCK_CADD_DATASET_HT,
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
                    cadd=1,
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
                        cadd=hl.Struct(assertion=['A', 'B']),
                        primate_ai=hl.Struct(new_enum=['A', 'B']),
                    ),
                    date=ANY,
                ),
            ],
        )
