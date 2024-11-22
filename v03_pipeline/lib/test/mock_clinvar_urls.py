import gzip
import tempfile
from contextlib import contextmanager

import pysam
import responses

from v03_pipeline.lib.model.definitions import ReferenceGenome
from v03_pipeline.lib.reference_datasets.clinvar import CLINVAR_SUBMISSION_SUMMARY_URL
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    ReferenceDataset,
)

CLINVAR_VCF = 'v03_pipeline/var/test/reference_data/clinvar.vcf'
CLINVAR_SUBMISSION_SUMMARY = (
    'v03_pipeline/var/test/reference_data/submission_summary.txt'
)


@contextmanager
def mock_clinvar_urls():
    with tempfile.NamedTemporaryFile(
        suffix='.vcf.bgz',
    ) as f1, open(CLINVAR_SUBMISSION_SUMMARY, 'rb') as f2:
        responses.add_passthru('http://localhost')
        # pysam is being used as it was the cleanest way to
        # get a bgzip formatted file :/
        pysam.tabix_compress(CLINVAR_VCF, f1.name, force=True)
        responses.get(
            ReferenceDataset.clinvar.raw_dataset_path(ReferenceGenome.GRCh38),
            body=f1.read(),
        )
        responses.get(
            CLINVAR_SUBMISSION_SUMMARY_URL,
            body=gzip.compress(f2.read()),
        )
        yield
