import hail as hl
import logging

from hail_scripts.computed_fields.variant_id import get_expr_for_variant_ids

logger = logging.getLogger()


def import_table(
        table_path: str,
        min_partitions: int = None,
        impute: bool = False,
        no_header: bool = False,
        force_bgz: bool = True,
        comment: str = "#",
        types: dict = None,
    ):

    logger.info(f"\n==> import table: {table_path}")

    ht = hl.import_table(
        table_path,
        impute=impute,
        no_header=no_header,
        force_bgz=force_bgz,
        comment=comment,
        types=types,
        min_partitions=min_partitions)

    ht = ht.annotate_globals(sourceFilePath=table_path)

    return ht


def import_vcf(
        vcf_path: str,
        genome_version: str,
        more_contig_recoding: dict = None,
        min_partitions: int = None,
        force_bgz: bool = True,
        drop_samples: bool = False,
        skip_invalid_loci: bool = False,
        split_multi_alleles: bool = True):
    """Import vcf and return MatrixTable.

    :param str vcf_path: MT to annotate with VEP
    :param str genome_version: "37" or "38"
    :param dict more_contig_recoding: add more contig recoding for importing VCF
    :param int min_partitions: min partitions
    :param bool force_bgz: read .gz as a bgzipped file
    :param bool drop_samples: if True, discard genotype info
    :param bool skip_invalid_loci: if True, skip loci that are not consistent with the reference_genome.
    """

    if genome_version not in ("37", "38"):
        raise ValueError(f"Invalid genome_version: {genome_version}")

    logger.info(f"\n==> import vcf: {vcf_path}")

    # add (or remove) "chr" prefix from vcf chroms so they match the reference
    ref = hl.get_reference(f"GRCh{genome_version}")
    contig_recoding = {
        **{ref_contig.replace("chr", ""): ref_contig for ref_contig in ref.contigs if "chr" in ref_contig},
        **{f"chr{ref_contig}": ref_contig for ref_contig in ref.contigs if "chr" not in ref_contig}}
    if more_contig_recoding:
        contig_recoding.update(more_contig_recoding)

    mt = hl.import_vcf(
        vcf_path,
        reference_genome=f"GRCh{genome_version}",
        contig_recoding=contig_recoding,
        min_partitions=min_partitions,
        force_bgz=force_bgz,
        drop_samples=drop_samples,
        skip_invalid_loci=skip_invalid_loci)

    mt = mt.annotate_globals(sourceFilePath=vcf_path, genomeVersion=genome_version)

    mt = mt.annotate_rows(
        original_alt_alleles=hl.or_missing(hl.len(mt.alleles) > 2, get_expr_for_variant_ids(mt.locus, mt.alleles))
    )

    if split_multi_alleles:
        mt = hl.split_multi_hts(mt)
        mt = mt.key_rows_by(**hl.min_rep(mt.locus, mt.alleles))

    return mt


def read_mt(mt_path: str):
    """Read MT from the given path."""
    logger.info(f"\n==> read in: {mt_path}")

    return hl.read_matrix_table(mt_path)


def write_mt(mt_or_ht, output_path: str, overwrite: bool = True):
    """Writes the given MatrixTable or Table to the given output path."""

    logger.info(f"\n==> write out: {output_path}")

    mt_or_ht.write(output_path, overwrite=overwrite)

write_ht = write_mt  # alias


def run_vep(
        mt: hl.MatrixTable,
        genome_version: str,
        name: str = 'vep',
        block_size: int = 1000,
        vep_config_json_path = None) -> hl.MatrixTable:
    """Runs VEP.

    :param MatrixTable mt: MT to annotate with VEP
    :param str genome_version: "37" or "38"
    :param str name: Name for resulting row field
    :param int block_size: Number of rows to process per VEP invocation.
    :return: annotated MT
    :rtype: MatrixTable
    """
    if vep_config_json_path is not None:
        config = vep_config_json_path
        mt = mt.annotate_globals(gencodeVersion="unknown")
    else:
        if genome_version not in ["37", "38"]:
            raise ValueError(f"Invalid genome version: {genome_version}")
        config = "file:///vep_data/vep-gcloud.json"

    mt = hl.vep(mt, config=config, name=name, block_size=block_size)

    logger.info("==> Done with VEP")
    return mt
