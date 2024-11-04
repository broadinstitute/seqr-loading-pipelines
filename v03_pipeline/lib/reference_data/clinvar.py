import gzip
import os
import shutil
import tempfile
import urllib

import hail as hl
import hailtop.fs as hfs
import requests

from v03_pipeline.lib.annotations.enums import CLINVAR_PATHOGENICITIES_LOOKUP
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.model import Env
from v03_pipeline.lib.model.definitions import ReferenceGenome
from v03_pipeline.lib.paths import clinvar_dataset_path

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
    'no_classification_for_the_single_variant',
    'no_classifications_from_unflagged_records',
]
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
    'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/submission_summary.txt.gz'
)
MIN_HT_PARTITIONS = 2000
logger = get_logger(__name__)


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


def get_clinvar_ht(
    clinvar_url: str,
    reference_genome: ReferenceGenome,
):
    etag = requests.head(clinvar_url, timeout=10).headers.get('ETag').strip('"')
    clinvar_ht_path = clinvar_dataset_path(reference_genome, etag)
    if hfs.exists(clinvar_ht_path):
        logger.info(f'Try using cached clinvar ht with etag {etag}')
        ht = hl.read_table(clinvar_ht_path)
    else:
        logger.info('Cached clinvar ht not found, downloading latest clinvar vcf')
        hl._set_flags(use_new_shuffle=None, no_whole_stage_codegen='1')  # noqa: SLF001
        ht = download_and_import_latest_clinvar_vcf(clinvar_url, reference_genome)
        write(ht, clinvar_ht_path, repartition=False)
        hl._set_flags(use_new_shuffle='1', no_whole_stage_codegen='1')  # noqa: SLF001
    return ht


def download_and_import_latest_clinvar_vcf(
    clinvar_url: str,
    reference_genome: ReferenceGenome,
) -> hl.Table:
    version = parse_clinvar_release_date(clinvar_url)
    with tempfile.NamedTemporaryFile(suffix='.vcf.gz', delete=False) as tmp_file:
        urllib.request.urlretrieve(clinvar_url, tmp_file.name)  # noqa: S310
        cached_tmp_file_name = os.path.join(
            Env.HAIL_TMP_DIR,
            os.path.basename(tmp_file.name),
        )
        # In cases where HAIL_TMP_DIR is a remote path, copy the
        # file there.  If it's local, do nothing.
        if tmp_file.name != cached_tmp_file_name:
            hfs.copy(tmp_file.name, cached_tmp_file_name)
        mt = hl.import_vcf(
            cached_tmp_file_name,
            reference_genome=reference_genome.value,
            drop_samples=True,
            skip_invalid_loci=True,
            contig_recoding=reference_genome.contig_recoding(include_mt=True),
            min_partitions=MIN_HT_PARTITIONS,
            force_bgz=True,
        )
        mt = mt.annotate_globals(version=version)
        return join_to_submission_summary_ht(mt.rows())


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


def join_to_submission_summary_ht(vcf_ht: hl.Table) -> hl.Table:
    # https://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/README - submission_summary.txt
    logger.info('Getting clinvar submission summary from NCBI FTP server')
    ht = download_and_import_clinvar_submission_summary()
    return vcf_ht.annotate(
        submitters=ht[vcf_ht.rsid].Submitters,
        conditions=ht[vcf_ht.rsid].Conditions,
    )


def download_and_import_clinvar_submission_summary() -> hl.Table:
    with tempfile.NamedTemporaryFile(
        suffix='.txt.gz',
        delete=False,
    ) as tmp_file, tempfile.NamedTemporaryFile(
        suffix='.txt',
        delete=False,
    ) as unzipped_tmp_file:
        urllib.request.urlretrieve(CLINVAR_SUBMISSION_SUMMARY_URL, tmp_file.name)  # noqa: S310
        # Unzip the gzipped file first to fix gzip files being read by hail with single partition
        with gzip.open(tmp_file.name, 'rb') as f_in, open(
            unzipped_tmp_file.name,
            'wb',
        ) as f_out:
            shutil.copyfileobj(f_in, f_out)
        cached_tmp_file_name = os.path.join(
            Env.HAIL_TMP_DIR,
            os.path.basename(unzipped_tmp_file.name),
        )
        # In cases where HAIL_TMP_DIR is a remote path, copy the
        # file there.  If it's local, do nothing.
        if unzipped_tmp_file.name != cached_tmp_file_name:
            hfs.copy(unzipped_tmp_file.name, cached_tmp_file_name)
        return import_submission_table(cached_tmp_file_name)


def import_submission_table(file_name: str) -> hl.Table:
    ht = hl.import_table(
        file_name,
        force=True,
        filter='^(#[^:]*:|^##).*$',  # removes all comments except for the header line
        types={
            '#VariationID': hl.tstr,
            'Submitter': hl.tstr,
            'ReportedPhenotypeInfo': hl.tstr,
        },
        missing='-',
        min_partitions=MIN_HT_PARTITIONS,
    )
    ht = ht.rename({'#VariationID': 'VariationID'})
    ht = ht.select('VariationID', 'Submitter', 'ReportedPhenotypeInfo')
    return ht.group_by('VariationID').aggregate(
        Submitters=hl.agg.collect(ht.Submitter),
        Conditions=hl.agg.collect(ht.ReportedPhenotypeInfo),
    )
