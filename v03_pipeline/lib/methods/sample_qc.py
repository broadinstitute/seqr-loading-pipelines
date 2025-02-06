import hail as hl
from gnomad.sample_qc.pipeline import filter_rows_for_qc


def call_sample_qc(
    mt: hl.MatrixTable,
):
    mt = mt.annotate_entries(
        GT=hl.case()
        .when(mt.GT.is_diploid(), hl.call(mt.GT[0], mt.GT[1], phased=False))
        .when(mt.GT.is_haploid(), hl.call(mt.GT[0], phased=False))
        .default(hl.missing(hl.tcall)),
    )
    return annotate_filtered_callrate(mt)


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
