import hail as hl

from v03_pipeline.lib.core import DatasetType, ReferenceGenome
from v03_pipeline.lib.misc.io import checkpoint
from v03_pipeline.lib.reference_datasets.misc import (
    download_zip_file,
    key_by_locus_alleles,
)

SHARED_TYPES = {
    'fathmm-MKL_coding_score': hl.tfloat32,
    'PrimateAI_score': hl.tfloat32,
}
TYPES = {
    ReferenceGenome.GRCh37: {
        **SHARED_TYPES,
        'hg19_pos(1-based)': hl.tint,
        'CADD_phred_hg19': hl.tfloat32,
    },
    ReferenceGenome.GRCh38: {
        **SHARED_TYPES,
        'pos(1-based)': hl.tint,
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
        'CADD_phred_hg19': 'CADD_phred',
    },
    ReferenceGenome.GRCh38: {
        **SHARED_RENAME,
        '#chr': 'chrom',
        'pos(1-based)': 'pos',
    },
}

PREDICTOR_SCORES = {
    'SIFT_score',
    'Polyphen2_HVAR_score',
    'VEST4_score',
    'MPC_score',
    'MutPred_score',
    'REVEL_score',
}
PREDICTOR_FIELDS = ['MutationTaster_pred']


def predictor_parse(field: hl.StringExpression) -> hl.StringExpression:
    return field.split(';').find(lambda p: p != '.')


def get_ht(path: str, reference_genome: ReferenceGenome) -> hl.Table:
    types = TYPES[reference_genome]
    rename = RENAME[reference_genome]

    with download_zip_file(path, 'dbnsfp') as unzipped_dir:
        ht = hl.import_table(
            f'{unzipped_dir}/dbNSFP*_variant.chr*.gz',
            types=types,
            missing='.',
            force=True,
        )
        # NB: We ran into weird issues...running out
        # of file descriptors on dataproc :/
        ht, _ = checkpoint(ht)
        hl._set_flags(use_new_shuffle=None, no_whole_stage_codegen='1')  # noqa: SLF001
        select_fields = {'ref', 'alt', *types.keys(), *rename.keys()}
        ht = ht.select(
            *select_fields,
            **{k: hl.parse_float32(predictor_parse(ht[k])) for k in PREDICTOR_SCORES},
            **{k: predictor_parse(ht[k]) for k in PREDICTOR_FIELDS},
        )
        ht = ht.rename(rename)
        ht = key_by_locus_alleles(ht, reference_genome)
        return ht.group_by(*ht.key).aggregate(
            **{f: hl.agg.take(ht[f], 1)[0] for f in PREDICTOR_FIELDS},
            **{f: hl.agg.max(ht[f]) for f in ht.row_value if f not in PREDICTOR_FIELDS},
        )


def select(_: ReferenceGenome, dataset_type: DatasetType, ht: hl.Table) -> hl.Table:
    if dataset_type == DatasetType.MITO:
        return ht.select(ht.SIFT_score, ht.MutationTaster_pred_id)
    return ht
