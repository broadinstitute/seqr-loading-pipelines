import tempfile
import urllib
import zipfile

import hail as hl

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_datasets.utils import key_by_locus_alleles

TYPES = {
    '#chr': hl.tstr,
    'pos(1-based)': hl.tint,
    'REVEL_score': hl.tfloat32,
}
TYPES_38 = {
    **TYPES,
    'fathmm-MKL_coding_score': hl.tfloat32,
    'MutPred_score': hl.tfloat32,
    'CADD_phred': hl.tfloat32,
}

RENAME = {
    '#chr': 'chrom',
    'pos(1-based)': 'pos',
    'fathmm-MKL_coding_score': 'fathmm_MKL_coding_score',
}

PREDICTOR_SCORES = {'REVEL_score', 'SIFT_score', 'Polyphen2_HVAR_score'}
PREDICTOR_SCORES_38 = {*PREDICTOR_SCORES, 'VEST4_score'}
PREDICTOR_FIELDS = ['MutationTaster_pred']


def predictor_parse(field: hl.StringExpression) -> hl.StringExpression:
    return field.split(';').find(lambda p: p != '.')

# adapted from download_and_create_reference_datasets/v02/hail_scripts/write_dbnsfp_ht.py
def get_ht(raw_dataset_path: str, reference_genome: ReferenceGenome) -> hl.Table:
    types = TYPES_38 if reference_genome == ReferenceGenome.GRCh38 else TYPES
    predictor_scores = PREDICTOR_SCORES_38 if reference_genome == ReferenceGenome.GRCh38 else PREDICTOR_SCORES
    with tempfile.TemporaryDirectory() as temp_dir:
        zip_path, _ = urllib.request.urlretrieve(raw_dataset_path)
        with zipfile.ZipFile(zip_path, 'r') as f:
            f.extractall(temp_dir)
            ht = hl.import_table(
            f'temp_dir/dbNSFP*_variant.chr*.gz',
                 types=types,
                 missing='.',
                 force=True,
                 min_partitions=10000,
            )

    ht = ht.filter(ht.alt != ht.ref)
    ht = ht.select(
        'ref', 'alt', *types.keys(),
        **{k: hl.parse_float32(predictor_parse(ht[k])) for k in predictor_scores},
        **{k: predictor_parse(ht[k]) for k in PREDICTOR_FIELDS},
    )
    ht = ht.rename(**{k: v for k, v in RENAME.items() if k in types})

    # We have to upper case alleles because 37 is known to have some non uppercases :(
    if reference_genome == ReferenceGenome.GRCh37:
        ht = ht.annotate(ref=ht.ref.upper(), alt=ht.alt.upper())

    return key_by_locus_alleles(ht, reference_genome)