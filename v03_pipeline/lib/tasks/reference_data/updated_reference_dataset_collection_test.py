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


class UpdatedReferenceDatasetCollectionTaskTest(MockedDatarootTestCase):
    @mock.patch(
        'v03_pipeline.lib.reference_data.dataset_table_operations.get_dataset_ht',
    )
    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_update_task_without_dataset_param(
        self,
        mock_reference_dataset_collection_datasets,
        mock_get_dataset_ht,
    ) -> None:
        mock_reference_dataset_collection_datasets.return_value = ['primate_ai', 'cadd']

        # mock tables for both datasets
        mock_get_dataset_ht.side_effect = [
            MOCK_PRIMATE_AI_DATASET_HT,
            hl.Table.parallelize(
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
                    enums=hl.Struct(),
                ),
            ),
        ]

        worker = luigi.worker.Worker()
        # create task with no dataset parameter
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
                    primate_ai=hl.Struct(score=0.5),
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
                    versions=hl.Struct(primate_ai='v0.2', cadd='v1.6'),
                    enums=hl.Struct(primate_ai=hl.Struct(), cadd=hl.Struct()),
                    date=ANY,
                ),
            ],
        )

    @mock.patch(
        'v03_pipeline.lib.reference_data.dataset_table_operations.get_dataset_ht',
    )
    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_update_task_with_dataset_and_empty_reference_data_table(
        self,
        mock_reference_dataset_collection_datasets,
        mock_get_dataset_ht,
    ) -> None:
        mock_reference_dataset_collection_datasets.return_value = ['primate_ai']
        mock_get_dataset_ht.return_value = MOCK_PRIMATE_AI_DATASET_HT

        worker = luigi.worker.Worker()
        # create task with dataset parameter
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

    @mock.patch(
        'v03_pipeline.lib.tasks.reference_data.updated_reference_dataset_collection.UpdatedReferenceDatasetCollectionTask.initialize_table',
    )
    @mock.patch(
        'v03_pipeline.lib.reference_data.dataset_table_operations.get_dataset_ht',
    )
    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_update_task_with_dataset_and_existing_reference_dataset_collection_table(
        self,
        mock_reference_dataset_collection_datasets,
        mock_get_dataset_ht,
        mock_initialize_table,
    ) -> None:
        # override initialize to read in existing reference dataset collection table
        mock_initialize_table.return_value = hl.read_table(COMBINED_2_PATH)
        mock_reference_dataset_collection_datasets.return_value = [
            'primate_ai',
            'cadd',
            'clinvar',
        ]
        mock_get_dataset_ht.return_value = MOCK_PRIMATE_AI_DATASET_HT

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
                    cadd=1,
                    clinvar=2,
                    primate_ai=hl.Struct(score=0.5),  # expect primate_ai to be updated
                ),
            ],
        )
        self.assertEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    paths=hl.Struct(
                        cadd='gs://seqr-reference-data/GRCh38/CADD/CADD_snvs_and_indels.v1.6.ht',
                        clinvar='ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz',
                        primate_ai='gs://seqr-reference-data/GRCh38/primate_ai/PrimateAI_scores_v0.2.liftover_grch38.ht',
                    ),
                    versions=hl.Struct(
                        cadd='v1.6',
                        clinvar='2023-07-02',
                        primate_ai='v0.2',
                    ),
                    enums=hl.Struct(
                        cadd=hl.Struct(),
                        clinvar=hl.Struct(
                            assertion=['Affects', 'association_not_found'],
                            pathogenicity=['Pathogenic', 'Benign'],
                        ),
                        primate_ai=hl.Struct(),
                    ),
                    date=ANY,
                ),
            ],
        )
