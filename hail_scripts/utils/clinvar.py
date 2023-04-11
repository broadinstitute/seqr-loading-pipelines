import gzip
import tempfile
import urllib.request

import hail as hl

from hail_scripts.utils.hail_utils import import_vcf

CLINVAR_FTP_PATH = "ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh{genome_version}/clinvar.vcf.gz"
CLINVAR_HT_PATH = "gs://seqr-reference-data/GRCh{genome_version}/clinvar/clinvar.GRCh{genome_version}.ht"

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

CLINVAR_SIGNIFICANCES_LOOKUP = {sig: i for i, sig in enumerate([
    'Pathogenic', 'Pathogenic,_risk_factor', 'Pathogenic,_Affects', 'Pathogenic,_drug_response',
    'Pathogenic,_drug_response,_protective,_risk_factor', 'Pathogenic,_association', 'Pathogenic,_other',
    'Pathogenic,_association,_protective', 'Pathogenic,_protective', 'Pathogenic/Likely_pathogenic',
    'Pathogenic/Likely_pathogenic,_risk_factor', 'Pathogenic/Likely_pathogenic,_drug_response',
    'Pathogenic/Likely_pathogenic,_other', 'Likely_pathogenic,_risk_factor', 'Likely_pathogenic',
    'Conflicting_interpretations_of_pathogenicity', 'Conflicting_interpretations_of_pathogenicity,_risk_factor',
    'Conflicting_interpretations_of_pathogenicity,_Affects',
    'Conflicting_interpretations_of_pathogenicity,_association,_risk_factor',
    'Conflicting_interpretations_of_pathogenicity,_other,_risk_factor',
    'Conflicting_interpretations_of_pathogenicity,_association',
    'Conflicting_interpretations_of_pathogenicity,_drug_response',
    'Conflicting_interpretations_of_pathogenicity,_drug_response,_other',
    'Conflicting_interpretations_of_pathogenicity,_other', 'Uncertain_significance',
    'Uncertain_significance,_risk_factor', 'Uncertain_significance,_Affects', 'Uncertain_significance,_association',
    'Uncertain_significance,_other', 'Affects', 'Affects,_risk_factor', 'Affects,_association', 'other', 'NA',
    'risk_factor', 'drug_response,_risk_factor', 'association', 'confers_sensitivity', 'drug_response', 'not_provided',
    'Likely_benign,_drug_response,_other', 'Likely_benign,_other', 'Likely_benign', 'Benign/Likely_benign,_risk_factor',
    'Benign/Likely_benign,_drug_response', 'Benign/Likely_benign,_other', 'Benign/Likely_benign', 'Benign,_risk_factor',
    'Benign,_confers_sensitivity', 'Benign,_association,_confers_sensitivity', 'Benign,_drug_response', 'Benign,_other',
    'Benign,_protective', 'Benign', 'protective,_risk_factor', 'protective',
    # In production - sort these correctly (get current list of values from actual clinvar file)
    'Pathogenic/Pathogenic,_low_penetrance', 'Pathogenic/Pathogenic,_low_penetrance|risk_factor',
    'Pathogenic/Likely_pathogenic/Pathogenic,_low_penetrance', 'Pathogenic/Likely_pathogenic/Pathogenic,_low_penetrance|other',
    'Pathogenic/Likely_pathogenic,_low_penetrance', 'Conflicting_interpretations_of_pathogenicity,_protective',
    'Likely_risk_allele', 'Uncertain_significance,_drug_response', 'Uncertain_significance/Uncertain_risk_allele',
    'Uncertain_risk_allele', 'Uncertain_risk_allele,_risk_factor', 'Uncertain_risk_allele,_protective',
    'association,_risk_factor', 'association,_drug_response', 'association,_drug_response,_risk_factor',
    'association_not_found', 'drug_response,_other', 'Likely_benign,_association', 'Benign/Likely_benign,_association',
    'Benign/Likely_benign,_drug_response,_other', 'Benign/Likely_benign,_other,_risk_factor', 'Benign,_association',
])}
# In production - use actual values in latest clinvar file version
CLINVAR_SIGNIFICANCES_LOOKUP.update({sig.replace(',_', '|'): i for sig, i in CLINVAR_SIGNIFICANCES_LOOKUP.items()})
CLINVAR_SIGNIFICANCES_LOOKUP = hl.dict(CLINVAR_SIGNIFICANCES_LOOKUP)


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
