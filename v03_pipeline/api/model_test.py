import unittest

from v03_pipeline.api.model import LoadingPipelineRequest
from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'


class ModelTest(unittest.TestCase):
    def test_valid_loading_pipeline_requests(self) -> None:
        raw_request = {
            'callset_path': TEST_VCF,
            'projects_to_run': ['project_a'],
            'sample_type': SampleType.WGS.value,
            'reference_genome': ReferenceGenome.GRCh38.value,
            'dataset_type': DatasetType.SNV_INDEL.value,
        }
        lpr = LoadingPipelineRequest.model_validate(raw_request)
        self.assertEqual(lpr.reference_genome, ReferenceGenome.GRCh38)

    def test_invalid_loading_pipeline_requests(self) -> None:
        raw_request = {
            'callset_path': 'a.txt',
            'projects_to_run': [],
            'sample_type': 'BLENDED',
            'reference_genome': ReferenceGenome.GRCh38.value,
            'dataset_type': DatasetType.SNV_INDEL.value,
        }
        with self.assertRaises(ValueError) as cm:
            LoadingPipelineRequest.model_validate(raw_request)
        self.assertTrue(
            str(cm.exception).startswith(
                '3 validation errors for LoadingPipelineRequest',
            ),
        )
