import hail as hl
from gnomad.sample_qc.pipeline import filter_rows_for_qc

from v03_pipeline.lib.model import SampleType

GNOMAD_FILTER_MIN_AF = 0.001
GNOMAD_FILTER_MIN_CALLRATE = 0.99

CALLRATE_LOW_THRESHOLD = 0.85
CONTAMINATION_UPPER_THRESHOLD = 5
WES_COVERAGE_LOW_THRESHOLD = 85
WGS_CALLRATE_LOW_THRESHOLD = 30


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
    return annotate_filter_flags(mt, tdr_metrics_ht, sample_type)


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
