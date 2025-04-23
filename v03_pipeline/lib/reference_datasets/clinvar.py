import gzip
import shutil
import tempfile

import hail as hl
import requests

from v03_pipeline.lib.annotations.enums import (
    CLINVAR_ASSERTIONS,
    CLINVAR_DEFAULT_PATHOGENICITY,
    CLINVAR_PATHOGENICITIES,
    CLINVAR_PATHOGENICITIES_LOOKUP,
)
from v03_pipeline.lib.model.definitions import ReferenceGenome
from v03_pipeline.lib.reference_datasets.misc import (
    BIALLELIC,
    copy_to_cloud_storage,
    vcf_to_ht,
)

CLINVAR_GOLD_STARS_LOOKUP = hl.dict(
    {
        'no_classification_for_the_single_variant': 0,
        'no_classification_provided': 0,
        'no_assertion_criteria_provided': 0,
        'no_classifications_from_unflagged_records': 0,
        'criteria_provided,_single_submitter': 1,
        'criteria_provided,_conflicting_classifications': 1,
        'criteria_provided,_multiple_submitters,_no_conflicts': 2,
        'reviewed_by_expert_panel': 3,
        'practice_guideline': 4,
    },
)
CLINVAR_SUBMISSION_SUMMARY_URL = (
    'https://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/submission_summary.txt.gz'
)

ENUMS = {
    'assertion': CLINVAR_ASSERTIONS,
    'pathogenicity': CLINVAR_PATHOGENICITIES,
}


def parsed_clnsig(ht: hl.Table):
    return (
        hl.delimit(ht.info.CLNSIG)
        .replace(
            'Likely_pathogenic,_low_penetrance',
            'Likely_pathogenic|low_penetrance',
        )
        .replace(
            '/Pathogenic,_low_penetrance/Established_risk_allele',
            '/Established_risk_allele|low_penetrance',
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


def parse_clinvar_release_date(clinvar_url: str) -> str:
    response = requests.get(clinvar_url, stream=True, timeout=10)
    for byte_line in gzip.GzipFile(fileobj=response.raw):
        line = byte_line.decode('ascii').strip()
        if not line:
            continue
        if line.startswith('##fileDate='):
            return line.split('=')[-1].strip()
        if not line.startswith('#'):
            return None
    return None


def get_submission_summary_ht() -> hl.Table:
    with (
        tempfile.NamedTemporaryFile(
            suffix='.txt.gz',
            delete=False,
        ) as tmp_file,
        requests.get(
            CLINVAR_SUBMISSION_SUMMARY_URL,
            stream=True,
            timeout=10,
        ) as r,
    ):
        shutil.copyfileobj(r.raw, tmp_file)
    cloud_tmp_file = copy_to_cloud_storage(tmp_file.name)
    ht = hl.import_table(
        cloud_tmp_file,
        force=True,
        filter='^(#[^:]*:|^##).*$',  # removes all comments except for the header line
        types={
            '#VariationID': hl.tstr,
            'Submitter': hl.tstr,
            'ReportedPhenotypeInfo': hl.tstr,
        },
        missing='-',
    )
    ht = ht.rename({'#VariationID': 'VariationID'})
    ht = ht.select('VariationID', 'Submitter', 'ReportedPhenotypeInfo')
    return ht.group_by('VariationID').aggregate(
        Submitters=hl.agg.collect(ht.Submitter),
        Conditions=hl.agg.collect(ht.ReportedPhenotypeInfo),
    )


def select_fields(ht):
    clnsigs = parsed_clnsig(ht)
    return ht.select(
        alleleId=ht.info.ALLELEID,
        pathogenicity=hl.if_else(
            CLINVAR_PATHOGENICITIES_LOOKUP.contains(clnsigs[0]),
            clnsigs[0],
            CLINVAR_DEFAULT_PATHOGENICITY,
        ),
        assertion=hl.if_else(
            CLINVAR_PATHOGENICITIES_LOOKUP.contains(clnsigs[0]),
            clnsigs[1:],
            clnsigs,
        ),
        # NB: there's a hidden enum-mapping inside this clinvar function.
        conflictingPathogenicities=parsed_and_mapped_clnsigconf(ht),
        goldStars=CLINVAR_GOLD_STARS_LOOKUP.get(hl.delimit(ht.info.CLNREVSTAT)),
        submitters=ht.submitters,
        # assumes the format 'MedGen#:condition;MedGen#:condition', e.g.'C0023264:Leigh syndrome'
        conditions=hl.filter(
            hl.is_defined,
            hl.flatmap(
                lambda p: p.split(';'),
                ht.conditions,
            )
            .filter(lambda x: x != '')
            .map(lambda p: p.split(':')[1]),
        ),
    )


def get_ht(
    clinvar_url: str,
    reference_genome: ReferenceGenome,
) -> hl.Table:
    with (
        tempfile.NamedTemporaryFile(
            suffix='.vcf.gz',
            delete=False,
        ) as tmp_file,
        requests.get(clinvar_url, stream=True, timeout=10) as r,
    ):
        shutil.copyfileobj(r.raw, tmp_file)
    cloud_tmp_file = copy_to_cloud_storage(tmp_file.name)
    ht = vcf_to_ht(cloud_tmp_file, reference_genome)
    # Filter deletions present as single alleles
    ht = ht.filter(hl.len(ht.alleles) == BIALLELIC)
    submitters_ht = get_submission_summary_ht()
    ht = ht.annotate(
        submitters=submitters_ht[ht.rsid].Submitters,
        conditions=submitters_ht[ht.rsid].Conditions,
    )
    return select_fields(ht)
