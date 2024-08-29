import re
from collections.abc import Callable

import hail as hl

from v03_pipeline.lib.misc.io import checkpoint
from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType, Sex

AMBIGUOUS_THRESHOLD_PERC: float = 0.01  # Fraction of samples identified as "ambiguous_sex" above which an error will be thrown.
MIN_ROWS_PER_CONTIG = 100
SAMPLE_TYPE_MATCH_THRESHOLD = 0.3


class SeqrValidationError(Exception):
    pass


def validated_hl_function(msg: str) -> Callable[[Callable], Callable]:
    def decorator(fn: Callable[[]]) -> Callable:
        def wrapper(*args, **kwargs) -> hl.Table | hl.MatrixTable:
            try:
                t, _ = checkpoint(fn(*args, **kwargs))
            except hl.utils.java.FatalError as e:
                hail_error = re.search('Error summary: (.*)$', str(e))
                if hail_error:
                    nonlocal msg
                    msg = msg + f'The error summary provided by HAIL: {hail_error}'
                raise SeqrValidationError(msg) from e
            else:
                return t

        return wrapper

    return decorator


def validate_allele_type(
    mt: hl.MatrixTable,
) -> None:
    ht = mt.rows()
    ht = ht.filter(
        hl.numeric_allele_type(ht.alleles[0], ht.alleles[1])
        == hl.genetics.allele_type.AlleleType.UNKNOWN,
    )
    if ht.count() > 0:
        msg = f'Alleles with Unknown AlleleType are present in the callset: {ht.alleles.collect()}'
        raise SeqrValidationError(msg)


def validate_no_duplicate_variants(
    mt: hl.MatrixTable,
) -> None:
    ht = mt.rows()
    ht = ht.group_by(*ht.key).aggregate(n=hl.agg.count())
    ht = ht.filter(ht.n > 1)
    if ht.count() > 0:
        msg = f'Variants are present multiple times in the callset: {ht.collect()}'
        raise SeqrValidationError(msg)


def validate_expected_contig_frequency(
    mt: hl.MatrixTable,
    reference_genome: ReferenceGenome,
    min_rows_per_contig: int = MIN_ROWS_PER_CONTIG,
) -> None:
    rows_per_contig = mt.aggregate_rows(hl.agg.counter(mt.locus.contig))
    missing_contigs = (
        reference_genome.standard_contigs
        - reference_genome.optional_contigs
        - rows_per_contig.keys()
    )
    if missing_contigs:
        msg = 'Missing the following expected contigs:{}'.format(
            ', '.join(missing_contigs),
        )
        raise SeqrValidationError(msg)

    for contig, count in rows_per_contig.items():
        if contig in reference_genome.optional_contigs:
            continue
        if count < min_rows_per_contig:
            msg = f'Contig {contig} has {count} rows, which is lower than expected minimum count {min_rows_per_contig}.'
            raise SeqrValidationError(msg)


def validate_imported_field_types(
    mt: hl.MatrixTable,
    dataset_type: DatasetType,
    additional_row_fields: dict[str, hl.expr.types.HailType | set],
) -> None:
    def _validate_field(
        mt_schema: hl.StructExpression,
        field: str,
        dtype: hl.expr.types.HailType,
    ) -> str | None:
        if field not in mt_schema:
            return f'{field}: missing'
        if (
            (
                dtype == hl.tstruct
                and type(mt_schema[field])
                == hl.expr.expressions.typed_expressions.StructExpression
            )
            or (isinstance(dtype, set) and mt_schema[field].dtype in dtype)
            or (mt_schema[field].dtype == dtype)
        ):
            return None
        return f'{field}: {mt_schema[field].dtype}'

    unexpected_field_types = []
    for field, dtype in dataset_type.col_fields.items():
        unexpected_field_types.append(_validate_field(mt.col, field, dtype))
    for field, dtype in dataset_type.entries_fields.items():
        unexpected_field_types.append(_validate_field(mt.entry, field, dtype))
    for field, dtype in {**dataset_type.row_fields, **additional_row_fields}.items():
        unexpected_field_types.append(_validate_field(mt.row, field, dtype))
    unexpected_field_types = [x for x in unexpected_field_types if x is not None]
    if unexpected_field_types:
        msg = f'Found unexpected field types on MatrixTable after import: {unexpected_field_types}'
        raise SeqrValidationError(msg)


def validate_imputed_sex_ploidy(
    mt: hl.MatrixTable,
    sex_check_ht: hl.Table,
) -> None:
    mt = mt.select_cols(
        discrepant=(
            (
                # All calls are diploid or missing but the sex is Male
                hl.agg.all(mt.GT.is_diploid() | hl.is_missing(mt.GT))
                & (sex_check_ht[mt.s].predicted_sex == Sex.MALE.value)
            )
            | (
                # At least one call is haploid but the sex is Female
                hl.agg.any(~mt.GT.is_diploid())
                & (sex_check_ht[mt.s].predicted_sex == Sex.FEMALE.value)
            )
        ),
    )
    discrepant_rate = mt.aggregate_cols(hl.agg.fraction(mt.discrepant))
    if discrepant_rate:
        msg = f'{discrepant_rate:.2%} of samples have misaligned ploidy with their provided imputed sex.'
        raise SeqrValidationError(msg)


def validate_sample_type(
    mt: hl.MatrixTable,
    coding_and_noncoding_variants_ht: hl.Table,
    reference_genome: ReferenceGenome,
    sample_type: SampleType,
    sample_type_match_threshold: float = SAMPLE_TYPE_MATCH_THRESHOLD,
) -> None:
    coding_variants_ht = coding_and_noncoding_variants_ht.filter(
        coding_and_noncoding_variants_ht.coding,
    )
    has_coding = (
        mt.semi_join_rows(coding_variants_ht).count_rows() / coding_variants_ht.count()
        >= sample_type_match_threshold
    )
    noncoding_variants_ht = coding_and_noncoding_variants_ht.filter(
        coding_and_noncoding_variants_ht.noncoding,
    )
    has_noncoding = (
        mt.semi_join_rows(noncoding_variants_ht).count_rows()
        / noncoding_variants_ht.count()
        >= sample_type_match_threshold
    )
    if not has_coding and not has_noncoding:
        msg = f"Genome version validation error: dataset specified as {reference_genome.value} but doesn't contain the expected number of common {reference_genome.value} variants"
        raise SeqrValidationError(msg)
    if has_noncoding and not has_coding:
        msg = f'Sample type validation error: dataset contains noncoding variants but is missing common coding variants for {reference_genome.value}. Please verify that the dataset contains coding variants.'
        raise SeqrValidationError(msg)
    if has_coding and not has_noncoding and sample_type != SampleType.WES:
        msg = 'Sample type validation error: dataset sample-type is specified as WGS but appears to be WES because it contains many common coding variants'
        raise SeqrValidationError(msg)
    if has_noncoding and has_coding and sample_type != SampleType.WGS:
        msg = 'Sample type validation error: dataset sample-type is specified as WES but appears to be WGS because it contains many common non-coding variants'
        raise SeqrValidationError(msg)
