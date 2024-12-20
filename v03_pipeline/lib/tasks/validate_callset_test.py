import json
import shutil

import luigi.worker

from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
    SampleType,
)
from v03_pipeline.lib.paths import (
    valid_reference_dataset_path,
)
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset
from v03_pipeline.lib.tasks.validate_callset import (
    ValidateCallsetTask,
)
from v03_pipeline.lib.tasks.write_validation_errors_for_run import (
    WriteValidationErrorsForRunTask,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_CODING_AND_NONCODING_HT = (
    'v03_pipeline/var/test/reference_datasets/GRCh38/gnomad_coding_and_noncoding/1.0.ht'
)
MULTIPLE_VALIDATION_EXCEPTIONS_VCF = (
    'v03_pipeline/var/test/callsets/multiple_validation_exceptions.vcf'
)

TEST_RUN_ID = 'manual__2024-04-03'


class ValidateCallsetTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        shutil.copytree(
            TEST_CODING_AND_NONCODING_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.gnomad_coding_and_noncoding,
            ),
        )

    def test_validate_callset_multiple_exceptions(
        self,
    ) -> None:
        worker = luigi.worker.Worker()
        validate_callset_task = ValidateCallsetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WES,
            # NB:
            # This callset contains duplicate rows for chr1:902088,
            # a NON_REF allele type at position chr1: 902024, missing
            # all contigs but chr1, and contains non-coding variants.
            callset_path=MULTIPLE_VALIDATION_EXCEPTIONS_VCF,
            project_guids=['project_a'],
            skip_validation=False,
            run_id=TEST_RUN_ID,
        )
        worker.add(validate_callset_task)
        worker.run()
        self.assertFalse(validate_callset_task.complete())

        write_validation_errors_task = WriteValidationErrorsForRunTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WES,
            callset_path=MULTIPLE_VALIDATION_EXCEPTIONS_VCF,
            project_guids=['project_a'],
            skip_validation=False,
            run_id=TEST_RUN_ID,
        )
        self.assertTrue(write_validation_errors_task.complete())
        with write_validation_errors_task.output().open('r') as f:
            self.assertDictEqual(
                json.load(f),
                {
                    'project_guids': ['project_a'],
                    'error_messages': [
                        'Alleles with invalid allele <NON_REF> are present in the callset.  This appears to be a GVCF containing records for sites with no variants.',
                        "Variants are present multiple times in the callset: ['1-902088-G-A']",
                        'Missing the following expected contigs:chr10, chr11, chr12, chr13, chr14, chr15, chr16, chr17, chr18, chr19, chr2, chr20, chr21, chr22, chr3, chr4, chr5, chr6, chr7, chr8, chr9, chrX',
                        'Sample type validation error: dataset sample-type is specified as WES but appears to be WGS because it contains many common non-coding variants',
                    ],
                    'failed_family_samples': {},
                },
            )
