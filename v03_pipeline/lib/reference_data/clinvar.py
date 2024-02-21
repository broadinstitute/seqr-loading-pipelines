import gzip
import os
import subprocess
import tempfile
import urllib

import hail as hl

from v03_pipeline.lib.annotations.enums import CLINVAR_PATHOGENICITIES_LOOKUP
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import Env
from v03_pipeline.lib.model.definitions import ReferenceGenome

CLINVAR_ASSERTIONS = [
    'Affects',
    'association',
    'association_not_found',
    'confers_sensitivity',
    'drug_response',
    'low_penetrance',
    'not_provided',
    'other',
    'protective',
    'risk_factor',
]
CLINVAR_GOLD_STARS_LOOKUP = hl.dict(
    {
        'no_classification_for_the_single_variant': 0,
        'no_assertion_provided': 0,
        'no_assertion_criteria_provided': 0,
        'criteria_provided,_single_submitter': 1,
        'criteria_provided,_conflicting_classifications': 1,
        'criteria_provided,_multiple_submitters,_no_conflicts': 2,
        'reviewed_by_expert_panel': 3,
        'practice_guideline': 4,
    },
)

logger = get_logger(__name__)


def safely_move_to_gcs(tmp_file_name, gcs_tmp_file_name):
    try:
        subprocess.run(
            [  # noqa: S603, S607
                'gsutil',
                'cp',
                tmp_file_name,
                gcs_tmp_file_name,
            ],
            check=True,
        )
    except subprocess.CalledProcessError:
        logger.exception(f'Failed to move local tmp file {tmp_file_name} to gcs')


def parsed_clnsig(ht: hl.Table):
    return (
        hl.delimit(ht.info.CLNSIG)
        .replace(
            'Likely_pathogenic,_low_penetrance',
            'Likely_pathogenic|low_penetrance',
        )
        .replace(
            '/Pathogenic,_low_penetrance',
            '|low_penetrance',
        )
        .split(r'\|')
    )


def parse_to_count(entry: str):
    splt = entry.split(
        r'\(',
    )  # pattern, count = entry... if destructuring worked on a hail expression!
    return hl.Struct(
        pathogenicity_id=CLINVAR_PATHOGENICITIES_LOOKUP[splt[0]],
        count=hl.int32(splt[1][:-1]),
    )


def parsed_and_mapped_clnsigconf(ht: hl.Table):
    return (
        hl.delimit(ht.info.CLNSIGCONF)
        .replace(',_low_penetrance', '')
        .split(r'\|')
        .map(parse_to_count)
        .group_by(lambda x: x.pathogenicity_id)
        .map_values(
            lambda values: (
                values.fold(
                    lambda x, y: x + y.count,
                    0,
                )
            ),
        )
        .items()
        .map(lambda x: hl.Struct(pathogenicity_id=x[0], count=x[1]))
    )


def download_and_import_latest_clinvar_vcf(
    clinvar_url: str,
    reference_genome: ReferenceGenome,
) -> hl.Table:
    """Downloads the latest clinvar VCF from the NCBI FTP server, imports it to a MT and returns that."""

    with tempfile.NamedTemporaryFile(suffix='.vcf.gz', delete=False) as tmp_file:
        urllib.request.urlretrieve(clinvar_url, tmp_file.name)  # noqa: S310
        gcs_tmp_file_name = os.path.join(
            Env.HAIL_TMPDIR, os.path.basename(tmp_file.name),
        )
        safely_move_to_gcs(tmp_file.name, gcs_tmp_file_name)
        mt = hl.import_vcf(
            gcs_tmp_file_name,
            reference_genome=reference_genome.value,
            drop_samples=True,
            skip_invalid_loci=True,
            contig_recoding=reference_genome.contig_recoding(include_mt=True),
            min_partitions=2000,
            force_bgz=True,
        )
        mt = mt.annotate_globals(version=_parse_clinvar_release_date(tmp_file.name))
        return mt.rows()


def _parse_clinvar_release_date(local_vcf_path: str) -> str:
    """Parse clinvar release date from the VCF header.

    Args:
        local_vcf_path (str): clinvar vcf path on the local file system.

    Returns:
        str: return VCF release date as string, or None if release date not found in header.
    """
    with gzip.open(local_vcf_path, 'rt') as f:
        for line in f:
            if line.startswith('##fileDate='):
                return line.split('=')[-1].strip()

            if not line.startswith('#'):
                return None

    return None
