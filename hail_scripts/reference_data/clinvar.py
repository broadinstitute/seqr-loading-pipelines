import gzip

import hail as hl
import requests

from hail_scripts.utils.hail_utils import import_vcf

CLINVAR_DEFAULT_PATHOGENICITY = 'No_pathogenic_assertion'
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
        'no_interpretation_for_the_single_variant': 0,
        'no_assertion_provided': 0,
        'no_assertion_criteria_provided': 0,
        'criteria_provided,_single_submitter': 1,
        'criteria_provided,_conflicting_interpretations': 1,
        'criteria_provided,_multiple_submitters,_no_conflicts': 2,
        'reviewed_by_expert_panel': 3,
        'practice_guideline': 4,
    },
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
CLINVAR_PATHOGENICITIES_LOOKUP = hl.dict(
    hl.enumerate(CLINVAR_PATHOGENICITIES, index_first=False),
)


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
    genome_version: str,
) -> hl.Table:
    """Downloads the latest clinvar VCF from the NCBI FTP server, imports it to a MT and returns that.

    Args:
        genome_version (str): "37" or "38"
    """

    if genome_version not in ['37', '38']:
        raise ValueError('Invalid genome_version: ' + str(genome_version))
    mt_contig_recoding = {'MT': 'chrM'} if genome_version == '38' else None
    mt = import_vcf(
        clinvar_url,
        genome_version,
        drop_samples=True,
        min_partitions=2000,
        skip_invalid_loci=True,
        more_contig_recoding=mt_contig_recoding,
    )
    mt = mt.annotate_globals(version=_parse_clinvar_release_date(clinvar_url))
    return mt.rows()


def _parse_clinvar_release_date(clinvar_url: str) -> str:
    """Parse clinvar release date from the VCF header.

    Args:
        clinvar_url (str): remote clinvar vcf path

    Returns:
        str: return VCF release date as string, or None if release date not found in header.
    """
    r = requests.get(clinvar_url, stream=True, timeout=30)
    with gzip.GzipFile(fileobj=r.raw) as f:
        for line_bytes in f:
            line = line_bytes.decode('utf8')
            if line.startswith('##fileDate='):
                clinvar_release_date = line.split('=')[-1].strip()
                return clinvar_release_date

            if not line.startswith('#'):
                return None

    return None
