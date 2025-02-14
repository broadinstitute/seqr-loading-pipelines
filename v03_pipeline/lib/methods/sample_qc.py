import pickle

import hail as hl
from gnomad.sample_qc.ancestry import assign_population_pcs, pc_project
from gnomad.sample_qc.filtering import compute_stratified_metrics_filter
from gnomad.sample_qc.pipeline import filter_rows_for_qc
from gnomad.utils.filtering import filter_to_autosomes

from v03_pipeline.lib.model import SampleType

CALLRATE_LOW_THRESHOLD = 0.85
CONTAMINATION_UPPER_THRESHOLD = 5
WES_COVERAGE_LOW_THRESHOLD = 85
WGS_CALLRATE_LOW_THRESHOLD = 30

POP_PCA_LOADINGS_PATH = (
    'gs://gcp-public-data--gnomad/release/4.0/pca/gnomad.v4.0.pca_loadings.ht'
)
ANCESTRY_RF_MODEL_PATH = 'v03_pipeline/var/ancestry_imputation_model.pickle'
NUM_PCS = 20

HAIL_QC_METRICS = [
    'n_snp',
    'r_ti_tv',
    'r_insertion_deletion',
    'n_insertion',
    'n_deletion',
    'r_het_hom_var',
]


def call_sample_qc(
    mt: hl.MatrixTable,
    tdr_metrics_ht: hl.Table,
    sample_type: SampleType,
):
    mt = mt.annotate_cols(sample_type=sample_type)
    mt = mt.annotate_entries(
        GT=hl.case()
        .when(mt.GT.is_diploid(), hl.call(mt.GT[0], mt.GT[1], phased=False))
        .when(mt.GT.is_haploid(), hl.call(mt.GT[0], phased=False))
        .default(hl.missing(hl.tcall)),
    )
    mt = annotate_filtered_callrate(mt)
    mt = annotate_filter_flags(mt, tdr_metrics_ht, sample_type)
    mt = annotate_qc_pop(mt)
    return run_hail_sample_qc(mt, sample_type)


def annotate_filtered_callrate(mt: hl.MatrixTable) -> hl.MatrixTable:
    filtered_mt = filter_rows_for_qc(
        mt,
        min_af=0.001,
        min_callrate=0.99,
        bi_allelic_only=True,
        snv_only=True,
        apply_hard_filters=False,
        min_inbreeding_coeff_threshold=None,
        min_hardy_weinberg_threshold=None,
    )
    callrate_ht = filtered_mt.select_cols(
        filtered_callrate=hl.agg.fraction(hl.is_defined(filtered_mt.GT)),
    ).cols()
    return mt.annotate_cols(**callrate_ht[mt.col_key])


def annotate_filter_flags(
    mt: hl.MatrixTable,
    tdr_metrics_ht: hl.Table,
    sample_type: SampleType,
) -> hl.MatrixTable:
    mt = mt.annotate_cols(**tdr_metrics_ht[mt.col_key])
    flags = {
        'callrate': mt.filtered_callrate < CALLRATE_LOW_THRESHOLD,
        'contamination': mt.contamination_rate > CONTAMINATION_UPPER_THRESHOLD,
    }
    if sample_type == SampleType.WES:
        flags['coverage'] = mt.percent_bases_at_20x < WES_COVERAGE_LOW_THRESHOLD
    else:
        flags['coverage'] = mt.mean_coverage < WGS_CALLRATE_LOW_THRESHOLD

    return mt.annotate_cols(
        filter_flags=hl.array(
            [hl.or_missing(filter_cond, name) for name, filter_cond in flags.items()],
        ).filter(hl.is_defined),
    ).drop(
        'contamination_rate',
        'percent_bases_at_20x',
        'mean_coverage',
        'filtered_callrate',
    )


def annotate_qc_pop(mt: hl.MatrixTable) -> hl.MatrixTable:
    mt = mt.select_entries('GT')
    scores = _get_pop_pca_scores(mt)
    with open(ANCESTRY_RF_MODEL_PATH, 'rb') as f:
        fit = pickle.load(f)  # noqa: S301

    pop_pca_ht, _ = assign_population_pcs(
        scores,
        pc_cols=scores.scores,
        output_col='qc_pop',
        fit=fit,
    )
    pop_pca_ht = pop_pca_ht.key_by('s')
    scores = scores.annotate(
        **{f'pop_PC{i + 1}': scores.scores[i] for i in range(NUM_PCS)},
    ).drop('scores', 'known_pop')
    pop_pca_ht = pop_pca_ht.annotate(**scores[pop_pca_ht.key])
    return mt.annotate_cols(**pop_pca_ht[mt.col_key])


def _get_pop_pca_scores(mt: hl.MatrixTable) -> hl.Table:
    loadings = hl.read_table(POP_PCA_LOADINGS_PATH)
    scores = pc_project(mt, loadings)
    return scores.annotate(scores=scores.scores[:NUM_PCS], known_pop='Unknown')


def run_hail_sample_qc(mt: hl.MatrixTable, sample_type: SampleType) -> hl.MatrixTable:
    mt = filter_to_autosomes(mt)
    mt = hl.split_multi_hts(mt)
    mt = hl.sample_qc(mt)
    mt = mt.annotate_cols(
        sample_qc=mt.sample_qc.annotate(
            f_inbreeding=hl.agg.inbreeding(mt.GT, mt['info.AF'][0]),
        ),
    )
    sample_qc_metrics = HAIL_QC_METRICS
    if sample_type == SampleType.WGS:
        sample_qc_metrics = [*sample_qc_metrics, 'call_rate']

    strat_ht = mt.cols()
    qc_metrics = {metric: strat_ht.sample_qc[metric] for metric in sample_qc_metrics}
    strata = {'qc_pop': strat_ht.qc_pop}

    metric_ht = compute_stratified_metrics_filter(strat_ht, qc_metrics, strata)
    metric_ht = metric_ht.annotate(
        sample_qc=mt.cols()[metric_ht.key].sample_qc,
        qc_metrics_filters=hl.array(metric_ht.qc_metrics_filters),
    )
    return mt.annotate_cols(**metric_ht[mt.col_key])
