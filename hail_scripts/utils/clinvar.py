import gzip
import tempfile
import urllib.request

import hail as hl

from hail_scripts.utils.hail_utils import import_vcf

CLINVAR_DEFAULT_PATHOGENICITY = 'No_pathogenic_assertion'
CLINVAR_FTP_PATH = "ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh{genome_version}/clinvar.vcf.gz"
CLINVAR_HT_PATH = "gs://seqr-reference-data/GRCh{genome_version}/clinvar/clinvar.GRCh{genome_version}.ht"

# NB: alphabetical
CLINVAR_ASSERTIONS =  [
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
# NB: sorted by pathogenicity
CLINVAR_PATHOGENICITIES = [
    'Pathogenic',
    'Pathogenic/Likely_pathogenic',
    'Pathogenic/Likely_pathogenic/Likely_risk_allele',
    'Pathogenic/Likely_risk_allele',
    'Likely_pathogenic',
    'Likely_pathogenic/Likely_risk_allele',
    'Established_risk_allele',
    'Likely_risk_allele',
    'Conflicting_interpretations_of_pathogenicity',
    'Uncertain_risk_allele',
    'Uncertain_significance/Uncertain_risk_allele',
    'Uncertain_significance',
    CLINVAR_DEFAULT_PATHOGENICITY,
    'Likely_benign',
    'Benign/Likely_benign',
    'Benign',
]

def parsed_clnsig(ht: hl.Table):
    return (
        hl.delimit(ht.info.CLNSIG)
        .replace(
            'Likely_pathogenic,_low_penetrance', 'Likely_pathogenic|low_penetrance',
        )
        .replace(
            '/Pathogenic,_low_penetrance', '|low_penetrance',
        )
        .split(r'\|')
    )

def parsed_clnsigconf(ht: hl.Table):

    def parse_to_count(entry: str):
        splt = entry.split(r'\(') # pattern, count = entry... if destructuring worked on a hail expression!
        return hl.tuple([
            splt[0], 
            hl.int32(splt[1][:-1])
        ])

    return (
        hl.delimit(ht.info.CLNSIGCONF)
        .replace(',_low_penetrance', '')
        .split(r'\|')
        .map(parse_to_count)
        .group_by(lambda x: x[0])
        .map_values(lambda values: (
            values.fold(
                lambda x, y: x + y[1],
                0,
            )
        ))
        .items()
    )

def download_and_import_latest_clinvar_vcf(genome_version: str, tmp_file: str) -> hl.MatrixTable:
    """Downloads the latest clinvar VCF from the NCBI FTP server, imports it to a MT and returns that.

    Args:
        genome_version (str): "37" or "38"
    """

    if genome_version not in ["37", "38"]:
        raise ValueError("Invalid genome_version: " + str(genome_version))
    clinvar_url = CLINVAR_FTP_PATH.format(genome_version=genome_version)
    urllib.request.urlretrieve(clinvar_url, tmp_file.name)
    clinvar_release_date = _parse_clinvar_release_date(tmp_file.name)
    mt_contig_recoding = {'MT': 'chrM'} if genome_version == '38' else None
    mt = import_vcf(
        tmp_file.name,
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
