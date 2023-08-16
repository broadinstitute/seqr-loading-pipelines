import hail as hl

from v03_pipeline.lib.model import ReferenceGenome

MIN_ROWS_PER_CONTIG = 100


class SeqrValidationError(Exception):
    pass


def validate_contigs(
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

    unexpected_contigs = rows_per_contig.keys() - reference_genome.standard_contigs
    if unexpected_contigs:
        msg = 'Found the following unexpected contigs:{}'.format(
            ', '.join(unexpected_contigs),
        )
        raise SeqrValidationError(msg)

    for contig, count in rows_per_contig.items():
        if contig in reference_genome.optional_contigs:
            continue
        if count < min_rows_per_contig:
            msg = f'Contig {contig} has {count} rows, which is lower than expected minimum count {min_rows_per_contig}.'
            raise SeqrValidationError(msg)
