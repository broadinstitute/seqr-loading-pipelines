import unittest
from unittest import mock

import luigi

from v03_pipeline.lib.model import (
    CachedReferenceDatasetQuery,
    DatasetType,
    ReferenceGenome,
    SampleType,
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
        kwargs = {
            'sample_type': SampleType.WGS,
            'callset_path': '',
            'project_guids': [],
            'project_remap_paths': [],
            'project_pedigree_paths': [],
            'skip_validation': True,
            'run_id': '1',
        }
        task = UpdateCachedReferenceDatasetQueries(
            reference_genome=ReferenceGenome.GRCh37,
            dataset_type=DatasetType.SNV_INDEL,
            **kwargs,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())
        call_args_list = mock_crdq_task.call_args_list
        self.assertEqual(len(call_args_list), 4)
        self.assertEqual(
            [x.kwargs['crdq'] for x in call_args_list],
            list(CachedReferenceDatasetQuery),
        )

    def test_38_snv_indel(self, mock_crdq_task):
        mock_crdq_task.return_value = MockCompleteTask()
        worker = luigi.worker.Worker()
        kwargs = {
            'sample_type': SampleType.WGS,
            'callset_path': '',
            'project_guids': [],
            'project_remap_paths': [],
            'project_pedigree_paths': [],
            'skip_validation': True,
            'run_id': '2',
        }
        task = UpdateCachedReferenceDatasetQueries(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            **kwargs,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())
        call_args_list = mock_crdq_task.call_args_list
        self.assertEqual(len(call_args_list), 4)
        self.assertEqual(
            [x.kwargs['crdq'] for x in call_args_list],
            list(CachedReferenceDatasetQuery),
        )

    def test_38_mito(self, mock_crdq_task):
        mock_crdq_task.return_value = MockCompleteTask()
        worker = luigi.worker.Worker()
        kwargs = {
            'sample_type': SampleType.WGS,
            'callset_path': '',
            'project_guids': [],
            'project_remap_paths': [],
            'project_pedigree_paths': [],
            'skip_validation': True,
            'run_id': '3',
        }
        task = UpdateCachedReferenceDatasetQueries(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.MITO,
            **kwargs,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())
        call_args_list = mock_crdq_task.call_args_list
        self.assertEqual(len(call_args_list), 1)
        self.assertEqual(
            next(x.kwargs['crdq'] for x in call_args_list),
            CachedReferenceDatasetQuery.CLINVAR_PATH_VARIANTS,
        )

    def test_38_sv(self, mock_crdq_task):
        mock_crdq_task.return_value = MockCompleteTask()
        worker = luigi.worker.Worker()
        kwargs = {
            'sample_type': SampleType.WGS,
            'callset_path': '',
            'project_guids': [],
            'project_remap_paths': [],
            'project_pedigree_paths': [],
            'skip_validation': True,
            'run_id': '4',
        }
        task = UpdateCachedReferenceDatasetQueries(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SV,
            **kwargs,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())
        # assert no crdq tasks for this reference genome and dataset type
        mock_crdq_task.assert_has_calls([])
