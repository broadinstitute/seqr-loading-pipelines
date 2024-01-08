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
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset_collection import (
    UpdatedReferenceDatasetCollectionTask,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

COMBINED_2_PATH = 'v03_pipeline/var/test/reference_data/test_combined_2.ht'
PRIMATE_AI_TEST_PATH = 'v03_pipeline/var/test/reference_data/primate_ai_test.ht'


class UpdatedReferenceDatasetCollectionTaskTest(MockedDatarootTestCase):
    @mock.patch(
        'v03_pipeline.lib.tasks.reference_data.updated_reference_dataset_collection.valid_reference_dataset_collection_path',
    )
    @mock.patch(
        'v03_pipeline.lib.reference_data.dataset_table_operations.get_dataset_ht',
    )
    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_update_with_dataset(
        self,
        mock_reference_dataset_collection_datasets,
        mock_get_dataset_ht,
        mock_destination_path,
    ) -> None:
        mock_reference_dataset_collection_datasets.return_value = ['primate_ai']
        # delete the dataset collection directory to fail initial task 'complete()' and force 'run()'
        # since collection won't exist, we can't test the codepath for updating an existing collection yet
        shutil.rmtree(COMBINED_2_PATH)
        mock_destination_path.return_value = COMBINED_2_PATH
        mock_get_dataset_ht.return_value = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    'alleles': ['A', 'C'],
                    'primate_ai': hl.Struct(score=0.5),
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
                version='v0.2',
                enums=hl.Struct(),
            ),
        )

        worker = luigi.worker.Worker()
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
                    primate_ai=hl.Struct(score=0.5),
                ),
            ],
        )
        self.assertEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    paths=hl.Struct(
                        primate_ai='gs://seqr-reference-data/GRCh38/primate_ai/PrimateAI_scores_v0.2.liftover_grch38.ht',
                    ),
                    versions=hl.Struct(primate_ai='v0.2'),
                    enums=hl.Struct(primate_ai=hl.Struct()),
                    date=ANY,
                ),
            ],
        )
