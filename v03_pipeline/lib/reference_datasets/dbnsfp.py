import tempfile
import urllib
import zipfile

import hail as hl

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_datasets.misc import key_by_locus_alleles

SHARED_TYPES = {
    'REVEL_score': hl.tfloat32,
    'fathmm-MKL_coding_score': hl.tfloat32,
    'MutPred_score': hl.tfloat32,
    'PrimateAI_score': hl.tfloat32,
}
TYPES = {
    ReferenceGenome.GRCh37: {
        **SHARED_TYPES,
        'pos(1-based)': hl.tint,
        'CADD_phred_hg19': hl.tfloat32,
    },
    ReferenceGenome.GRCh38: {
        **SHARED_TYPES,
        'hg19_pos(1-based)': hl.tint,
        'CADD_phred': hl.tfloat32,
    },
}

SHARED_RENAME = {
    'fathmm-MKL_coding_score': 'fathmm_MKL_coding_score',
}
RENAME = {
    ReferenceGenome.GRCh37: {
        **SHARED_RENAME,
        'hg19_chr': 'chrom',
        'hg19_pos(1-based)': 'pos',
    },
    ReferenceGenome.GRCh38: {
        **SHARED_RENAME,
        '#chr': 'chrom',
        'pos(1-based)': 'pos',
    },
}

PREDICTOR_SCORES = {
    'REVEL_score',
    'SIFT_score',
    'Polyphen2_HVAR_score',
    'VEST4_score',
    'MPC_score',
}
PREDICTOR_FIELDS = ['MutationTaster_pred']


def predictor_parse(field: hl.StringExpression) -> hl.StringExpression:
    return field.split(';').find(lambda p: p != '.')


# adapted from download_and_create_reference_datasets/v02/hail_scripts/write_dbnsfp_ht.py
def get_ht(raw_dataset_path: str, reference_genome: ReferenceGenome) -> hl.Table:
    types = TYPES[reference_genome]
    rename = RENAME[reference_genome]

    with tempfile.TemporaryDirectory() as temp_dir:
        zip_path, _ = urllib.request.urlretrieve(raw_dataset_path)
        with zipfile.ZipFile(zip_path, 'r') as f:
            f.extractall(temp_dir)

        ht = hl.import_table(
            f'{temp_dir}/dbNSFP*_variant.chr*.gz',
            types=types,
            missing='.',
            force=True,
        )
        select_fields = {'ref', 'alt', *types.keys(), *rename.keys()}
        ht = ht.select(
            *select_fields,
            **{k: hl.parse_float32(predictor_parse(ht[k])) for k in PREDICTOR_SCORES},
            **{k: predictor_parse(ht[k]) for k in PREDICTOR_FIELDS},
        )
        ht = ht.rename(**rename)

        return key_by_locus_alleles(ht, reference_genome)
