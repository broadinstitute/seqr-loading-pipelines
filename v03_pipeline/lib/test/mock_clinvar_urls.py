from contextlib import contextmanager

import responses

from v03_pipeline.lib.model.definitions import ReferenceGenome
from v03_pipeline.lib.reference_datasets.clinvar import CLINVAR_SUBMISSION_SUMMARY_URL
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    ReferenceDataset,
)

CLINVAR_VCF = 'v03_pipeline/var/test/reference_data/clinvar.vcf.gz'
CLINVAR_SUBMISSION_SUMMARY = (
    'v03_pipeline/var/test/reference_data/submission_summary.txt.gz'
)


@contextmanager
def mock_clinvar_urls():
    with open(CLINVAR_VCF, 'rb') as f, open(CLINVAR_SUBMISSION_SUMMARY, 'rb') as f2:
        responses.add_passthru('http://localhost')
        responses.get(
            ReferenceDataset.clinvar.raw_dataset_path(ReferenceGenome.GRCh38),
            body=f.read(),
        )
        responses.get(
            CLINVAR_SUBMISSION_SUMMARY_URL,
            body=f2.read(),
        )
        yield
