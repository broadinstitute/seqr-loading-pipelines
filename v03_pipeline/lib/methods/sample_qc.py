import hail as hl
import onnx
from gnomad.sample_qc.ancestry import assign_population_pcs, pc_project
from gnomad.sample_qc.pipeline import filter_rows_for_qc
from gnomad_qc.v4.sample_qc.assign_ancestry import assign_pop_with_per_pop_probs

from v03_pipeline.lib.model import SampleType

GNOMAD_FILTER_MIN_AF = 0.001
GNOMAD_FILTER_MIN_CALLRATE = 0.99

CALLRATE_LOW_THRESHOLD = 0.85
CONTAMINATION_UPPER_THRESHOLD = 5
WES_COVERAGE_LOW_THRESHOLD = 85
WGS_CALLRATE_LOW_THRESHOLD = 30

POP_PCA_LOADINGS_PATH = (
    'gs://gcp-public-data--gnomad/release/4.0/pca/gnomad.v4.0.pca_loadings.ht'
)
ANCESTRY_RF_MODEL_PATH = (
    'gs://seqr-reference-data/v3.1/GRCh38/SNV_INDEL/ancestry_imputation_model.onnx'
)
NUM_PCS = 20
GNOMAD_POP_PROBABILITY_CUTOFFS = {
    'afr': 0.93,
    'ami': 0.98,
    'amr': 0.89,
    'asj': 0.94,
    'eas': 0.95,
    'fin': 0.92,
    'mid': 0.55,
    'nfe': 0.75,
    'sas': 0.92,
}
POPULATION_MISSING_LABEL = 'oth'


def call_sample_qc(
    mt: hl.MatrixTable,
    tdr_metrics_ht: hl.Table,
    sample_type: SampleType,
):
    mt = mt.annotate_entries(
        GT=hl.case()
        .when(mt.GT.is_diploid(), hl.call(mt.GT[0], mt.GT[1], phased=False))
        .when(mt.GT.is_haploid(), hl.call(mt.GT[0], phased=False))
        .default(hl.missing(hl.tcall)),
    )
    mt = annotate_filtered_callrate(mt)
    mt = annotate_filter_flags(mt, tdr_metrics_ht, sample_type)
    return annotate_qc_gen_anc(mt)


def annotate_filtered_callrate(mt: hl.MatrixTable) -> hl.MatrixTable:
    filtered_mt = filter_rows_for_qc(
        mt,
        min_af=GNOMAD_FILTER_MIN_AF,
        min_callrate=GNOMAD_FILTER_MIN_CALLRATE,
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
    )


def annotate_qc_gen_anc(mt: hl.MatrixTable) -> hl.MatrixTable:
    mt = mt.select_entries('GT')
    scores = _get_pop_pca_scores(mt)
    with hl.hadoop_open(ANCESTRY_RF_MODEL_PATH, 'rb') as f:
        fit = onnx.load(f)

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
    pop_pca_ht = assign_pop_with_per_pop_probs(
        pop_pca_ht,
        min_prob_cutoffs=GNOMAD_POP_PROBABILITY_CUTOFFS,
        missing_label=POPULATION_MISSING_LABEL,
    )
    pop_pca_ht = pop_pca_ht.transmute(gq_gen_anc=pop_pca_ht.pop).drop('qc_pop')
    return mt.annotate_cols(**pop_pca_ht[mt.col_key])


def _get_pop_pca_scores(mt: hl.MatrixTable) -> hl.Table:
    loadings = hl.read_table(POP_PCA_LOADINGS_PATH)
    scores = pc_project(mt, loadings)
    return scores.annotate(scores=scores.scores[:NUM_PCS], known_pop='Unknown')
