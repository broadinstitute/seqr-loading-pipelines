import gzip
import tempfile
import urllib.request

import hail as hl

from hail_scripts.utils.hail_utils import import_vcf

CLINVAR_FTP_PATH = "ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh{genome_version}/clinvar.vcf.gz"
CLINVAR_HT_PATH = "gs://seqr-reference-data/GRCh{genome_version}/clinvar/clinvar.GRCh{genome_version}.ht"

CLINVAR_CLINICAL_SIGNIFICANCES_LOOKUP = hl.dict(
    hl.enumerate(
        # NB: sorted by pathogenicity
        [
            'Pathogenic',
            'Pathogenic/Likely_pathogenic',
            'Likely_pathogenic',
            'Likely_risk_allele',
            'Conflicting_interpretations_of_pathogenicity',
            'Uncertain_risk_allele',
            'Uncertain_significance',
            'Likely_benign',
            'Benign/Likely_benign',
            'Benign',
        ],
        index_first=False
    )
)
CLINVAR_CLINICAL_SIGNIFICANCE_MODIFIERS_LOOKUP = hl.dict(
    hl.enumerate(
        # NB: alphabetical
        [
            'Affects',
            'association',
            'association_not_found',
            'confers_sensitivity',
            'drug_response',
            'low_penetrance',
            'NA',
            'not_provided',
            'other',
            'protective',
        ],
        index_first=False
    ),
)
CLINVAR_GOLD_STARS_LOOKUP = hl.dict(
    {
        "no_interpretation_for_the_single_variant": 0,
        "no_assertion_provided": 0,
        "no_assertion_criteria_provided": 0,
        "criteria_provided,_single_submitter": 1,
        "criteria_provided,_conflicting_interpretations": 1,
        "criteria_provided,_multiple_submitters,_no_conflicts": 2,
        "reviewed_by_expert_panel": 3,
        "practice_guideline": 4,
    }
)

def download_and_import_latest_clinvar_vcf(genome_version: str) -> hl.MatrixTable:
    """Downloads the latest clinvar VCF from the NCBI FTP server, imports it to a MT and returns that.

    Args:
        genome_version (str): "37" or "38"
    """

    if genome_version not in ["37", "38"]:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    clinvar_url = CLINVAR_FTP_PATH.format(genome_version=genome_version)
    with tempfile.NamedTemporaryFile(delete=False, suffix='.vcf.gz') as local_tmp_file:
        urllib.request.urlretrieve(clinvar_url, local_tmp_file.name)
        clinvar_release_date = _parse_clinvar_release_date(local_tmp_file.name)
        mt_contig_recoding = {'MT': 'chrM'} if genome_version == '38' else None
        mt = import_vcf(
            local_tmp_file.name,
            genome_version,
            drop_samples=True,
            min_partitions=2000,
            skip_invalid_loci=True,
            more_contig_recoding=mt_contig_recoding
        )
    return mt.annotate_globals(version=clinvar_release_date)

def _parse_clinvar_release_date(local_vcf_path: str) -> str:
    """Parse clinvar release date from the VCF header.

    Args:
        local_vcf_path (str): clinvar vcf path on the local file system.

    Returns:
        str: return VCF release date as string, or None if release date not found in header.
    """
    with gzip.open(local_vcf_path, "rt") as f:
        for line in f:
            if line.startswith("##fileDate="):
                clinvar_release_date = line.split("=")[-1].strip()
                return clinvar_release_date

            if not line.startswith("#"):
                return None

    return None
