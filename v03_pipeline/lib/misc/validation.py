from typing import Any

import hail as hl

from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
    SampleType,
    Sex,
)

AMBIGUOUS_THRESHOLD_PERC: float = 0.01  # Fraction of samples identified as "ambiguous_sex" above which an error will be thrown.
MIN_ROWS_PER_CONTIG = 100
SAMPLE_TYPE_MATCH_THRESHOLD = 0.3


class SeqrValidationError(Exception):
    pass


def validate_allele_type(
    t: hl.Table | hl.MatrixTable,
    dataset_type: DatasetType,
    **_: Any,
) -> None:
    ht = t.rows() if isinstance(t, hl.MatrixTable) else t
    ht = ht.filter(
        dataset_type.invalid_allele_types.contains(
            hl.numeric_allele_type(ht.alleles[0], ht.alleles[1]),
        ),
    )
    if ht.count() > 0:
        collected_alleles = sorted(
            [tuple(x) for x in ht.aggregate(hl.agg.collect_as_set(ht.alleles))],
        )
        # Handle case where all invalid alleles are NON_REF, indicating a gvcf:
        if all('<NON_REF>' in alleles for alleles in collected_alleles):
            msg = 'Alleles with invalid allele <NON_REF> are present in the callset.  This appears to be a GVCF containing records for sites with no variants.'
            raise SeqrValidationError(msg)
        msg = f'Alleles with invalid AlleleType are present in the callset: {collected_alleles[:10]}'
        raise SeqrValidationError(msg)


def validate_no_duplicate_variants(
    t: hl.Table | hl.MatrixTable,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    **_: Any,
) -> None:
    ht = t.rows() if isinstance(t, hl.MatrixTable) else t
    ht = ht.group_by(*ht.key).aggregate(n=hl.agg.count())
    ht = ht.filter(ht.n > 1)
    ht = ht.select()
    if ht.count() > 0:
        variant_format = dataset_type.table_key_format_fn(reference_genome)
        msg = f'Variants are present multiple times in the callset: {[variant_format(v) for v in ht.take(10)]}'
        raise SeqrValidationError(msg)


def validate_expected_contig_frequency(
    mt: hl.MatrixTable,
    reference_genome: ReferenceGenome,
    min_rows_per_contig: int = MIN_ROWS_PER_CONTIG,
    **_: Any,
) -> None:
    rows_per_contig = mt.aggregate_rows(hl.agg.counter(mt.locus.contig))
    missing_contigs = (
        reference_genome.standard_contigs
        - reference_genome.optional_contigs
        - rows_per_contig.keys()
    )
    if missing_contigs:
        msg = 'Missing the following expected contigs:{}'.format(
            ', '.join(sorted(missing_contigs)),
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
    **_: Any,
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
    # NB: sex_check_ht will be undefined if sex checking is disabled for the run
    sex_check_ht: hl.Table | None = None,
    **_: Any,
) -> None:
    if not sex_check_ht:
        return
    mt = mt.select_cols(
        s=mt.s,
        discrepant=(
            (
                # All calls are diploid or missing but the sex is Male
                hl.agg.all(mt.GT.is_diploid() | hl.is_missing(mt.GT))
                & (sex_check_ht[mt.s].predicted_sex == Sex.MALE.value)
            )
            | (
                # At least one call is haploid but the sex is Female, X0, XXY, XYY, or XXX
                hl.agg.any(~mt.GT.is_diploid())
                & hl.literal(
                    {
                        Sex.FEMALE.value,
                        Sex.X0.value,
                        Sex.XYY.value,
                        Sex.XXY.value,
                        Sex.XXX.value,
                    },
                ).contains(sex_check_ht[mt.s].predicted_sex)
            )
        ),
    )
    discrepant_samples = mt.aggregate_cols(
        hl.agg.filter(mt.discrepant, hl.agg.collect_as_set(mt.s)),
    )
    if discrepant_samples:
        msg = f'Found samples with misaligned ploidy with their provided imputed sex: {sorted(discrepant_samples)}'
        raise SeqrValidationError(msg)


def validate_sample_type(
    mt: hl.MatrixTable,
    coding_and_noncoding_variants_ht: hl.Table,
    reference_genome: ReferenceGenome,
    sample_type: SampleType,
    sample_type_match_threshold: float = SAMPLE_TYPE_MATCH_THRESHOLD,
    **_: Any,
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
        msg = 'Sample type validation error: dataset sample-type is specified as WGS but appears to be WES because it contains many common coding variants but is missing common non-coding variants'
        raise SeqrValidationError(msg)
    if has_noncoding and has_coding and sample_type != SampleType.WGS:
        msg = 'Sample type validation error: dataset sample-type is specified as WES but appears to be WGS because it contains many common non-coding variants'
        raise SeqrValidationError(msg)
