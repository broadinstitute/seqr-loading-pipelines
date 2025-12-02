import hail as hl

from v03_pipeline.lib.core import DatasetType, ReferenceGenome
from v03_pipeline.lib.reference_datasets.misc import (
    key_by_locus_alleles,
)


GRCh38_FIELDS = {
    'chrom': '#chr',
    'pos': 'pos(1-based)',
    'ref': 'ref',
    'alt': 'alt',
    'cadd': 'CADD_phred',
    'eigen': 'Eigen-phred_coding',
    'fathmm': 'fathmm-XF_coding_score',
    'mpc': 'MPC_score',
    'mut_pred': 'MutPred_score',
    'mut_tester': 'MutationTaster_pred',
    'polyphen': 'Polyphen2_HVAR_score',
    'primate_ai': 'PrimateAI_score',
    'revel': 'REVEL_score',
    'sift': 'SIFT_score',
    'vest': 'VEST4_score',
}
GRCh37_FIELDS = {
    'hg19_pos(1-based)': 'pos(1-based)',
    **GRCh38_FIELDS,
}


def get_ht(path: str, reference_genome: ReferenceGenome) -> hl.Table:
    types = TYPES[reference_genome]
    rename = RENAME[reference_genome]
    ht = hl.import_table(path, force_bgz=True)
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
