import shutil
from unittest.mock import Mock, patch

import luigi.worker

from v03_pipeline.lib.model import (
    CachedReferenceDatasetQuery,
    DatasetType,
    ReferenceGenome,
    SampleType,
)
from v03_pipeline.lib.paths import (
    cached_reference_dataset_query_path,
)
from v03_pipeline.lib.tasks.validate_callset import (
    ValidateCallsetTask,
)
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_CODING_NONCODING_CRDQ_1 = (
    'v03_pipeline/var/test/reference_data/test_gnomad_coding_noncoding_crdq_1.ht'
)
TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'

TEST_RUN_ID = 'manual__2024-04-03'


class ValidateCallsetTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        shutil.copytree(
            TEST_CODING_NONCODING_CRDQ_1,
            cached_reference_dataset_query_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                CachedReferenceDatasetQuery.GNOMAD_CODING_AND_NONCODING_VARIANTS,
            ),
        )

    @patch(
        'v03_pipeline.lib.tasks.validate_callset.UpdatedCachedReferenceDatasetQuery',
    )
    def test_validate_callset_multiple_exceptions(
        self,
        mock_updated_cached_reference_dataset_query: Mock,
    ) -> None:
        mock_updated_cached_reference_dataset_query.return_value = MockCompleteTask()
        worker = luigi.worker.Worker()
        validate_callset_task = ValidateCallsetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            skip_validation=False,
            run_id=TEST_RUN_ID,
        )
        worker.add(validate_callset_task)
        worker.run()
