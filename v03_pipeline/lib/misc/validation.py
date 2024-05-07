import hail as hl

from v03_pipeline.lib.model import ReferenceGenome, SampleType, Sex

AMBIGUOUS_THRESHOLD_PERC: float = 0.01  # Fraction of samples identified as "ambiguous_sex" above which an error will be thrown.
MIN_ROWS_PER_CONTIG = 100
SAMPLE_TYPE_MATCH_THRESHOLD = 0.3


class SeqrValidationError(Exception):
    pass


def validate_ambiguous_sex(ht: hl.Table) -> None:
    ambiguous_perc = ht.aggregate(
        hl.agg.fraction(ht.predicted_sex == Sex.UNKNOWN.value),
    )
    if ambiguous_perc > AMBIGUOUS_THRESHOLD_PERC:
        msg = f'{ambiguous_perc:.2%} of samples identified as ambiguous.  Please contact the methods team to investigate the callset.'
        raise ValueError(msg)


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
