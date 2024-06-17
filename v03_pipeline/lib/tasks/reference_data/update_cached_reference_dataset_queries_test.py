import unittest
from unittest import mock

import luigi

from v03_pipeline.lib.model import (
    CachedReferenceDatasetQuery,
    DatasetType,
    ReferenceGenome,
)
from v03_pipeline.lib.tasks.reference_data.update_cached_reference_dataset_queries import (
    UpdateCachedReferenceDatasetQueries,
)
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask


@mock.patch(
    'v03_pipeline.lib.tasks.reference_data.update_cached_reference_dataset_queries.UpdatedCachedReferenceDatasetQuery',
)
class UpdateCachedReferenceDatasetQueriesTest(unittest.TestCase):
    def test_37_snv_indel(self, mock_crdq_task):
        mock_crdq_task.return_value = MockCompleteTask()
        worker = luigi.worker.Worker()
        task = UpdateCachedReferenceDatasetQueries(
            reference_genome=ReferenceGenome.GRCh37,
            dataset_type=DatasetType.SNV_INDEL,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())
        mock_crdq_task.assert_has_calls(
            [
                mock.call(
                    reference_genome=ReferenceGenome.GRCh37,
                    dataset_type=DatasetType.SNV_INDEL,
                    crdq=CachedReferenceDatasetQuery.CLINVAR_PATH_VARIANTS,
                ),
                mock.call(
                    reference_genome=ReferenceGenome.GRCh37,
                    dataset_type=DatasetType.SNV_INDEL,
                    crdq=CachedReferenceDatasetQuery.GNOMAD_CODING_AND_NONCODING_VARIANTS,
                ),
                mock.call(
                    reference_genome=ReferenceGenome.GRCh37,
                    dataset_type=DatasetType.SNV_INDEL,
                    crdq=CachedReferenceDatasetQuery.GNOMAD_QC,
                ),
                mock.call(
                    reference_genome=ReferenceGenome.GRCh37,
                    dataset_type=DatasetType.SNV_INDEL,
                    crdq=CachedReferenceDatasetQuery.HIGH_AF_VARIANTS,
                ),
            ],
        )

    def test_38_snv_indel(self, mock_crdq_task):
        mock_crdq_task.return_value = MockCompleteTask()
        worker = luigi.worker.Worker()
        task = UpdateCachedReferenceDatasetQueries(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())
        mock_crdq_task.assert_has_calls(
            [
                mock.call(
                    reference_genome=ReferenceGenome.GRCh38,
                    dataset_type=DatasetType.SNV_INDEL,
                    crdq=CachedReferenceDatasetQuery.CLINVAR_PATH_VARIANTS,
                ),
                mock.call(
                    reference_genome=ReferenceGenome.GRCh38,
                    dataset_type=DatasetType.SNV_INDEL,
                    crdq=CachedReferenceDatasetQuery.GNOMAD_CODING_AND_NONCODING_VARIANTS,
                ),
                mock.call(
                    reference_genome=ReferenceGenome.GRCh38,
                    dataset_type=DatasetType.SNV_INDEL,
                    crdq=CachedReferenceDatasetQuery.GNOMAD_QC,
                ),
                mock.call(
                    reference_genome=ReferenceGenome.GRCh38,
                    dataset_type=DatasetType.SNV_INDEL,
                    crdq=CachedReferenceDatasetQuery.HIGH_AF_VARIANTS,
                ),
            ],
        )

    def test_38_mito(self, mock_crdq_task):
        mock_crdq_task.return_value = MockCompleteTask()
        worker = luigi.worker.Worker()
        task = UpdateCachedReferenceDatasetQueries(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.MITO,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())
        mock_crdq_task.assert_has_calls(
            [
                mock.call(
                    reference_genome=ReferenceGenome.GRCh38,
                    dataset_type=DatasetType.MITO,
                    crdq=CachedReferenceDatasetQuery.CLINVAR_PATH_VARIANTS,
                ),
            ],
        )

    def test_38_sv(self, mock_crdq_task):
        mock_crdq_task.return_value = MockCompleteTask()
        worker = luigi.worker.Worker()
        task = UpdateCachedReferenceDatasetQueries(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SV,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())
        # assert no crdq tasks for this reference genome and dataset type
        mock_crdq_task.assert_has_calls([])
