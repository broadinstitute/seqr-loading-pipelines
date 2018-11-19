#!/usr/bin/env python3

# combine the pre-computed CADD .tsvs from https://cadd.gs.washington.edu/download into 1 Table for each genome build

import logging
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


import hail as hl
from hail_scripts.v02.utils.hail_utils import write_ht, import_table

hl.init()


def import_cadd_table(path: str, genome_version: str) -> hl.Table:
    if genome_version not in ("37", "38"):
        raise ValueError(f"Invalid genome version: {genome_version}")

    column_names = {'f0': 'chrom', 'f1': 'pos', 'f2': 'ref', 'f3': 'alt', 'f4': 'RawScore', 'f5': 'PHRED'}
    types = {'f0': hl.tstr, 'f1': hl.tint, 'f4': hl.tfloat32, 'f5': hl.tfloat32}

    cadd_ht = import_table(path, force_bgz=True, comment="#", no_header=True, types=types, min_partitions=2000)
    cadd_ht = cadd_ht.rename(column_names)

    chrom = hl.format("chr%s", cadd_ht.chrom) if genome_version == "38" else cadd_ht.chrom
    locus = hl.locus(chrom, cadd_ht.pos, reference_genome=hl.get_reference(f"GRCh{genome_version}"))
    alleles = hl.array([cadd_ht.ref, cadd_ht.alt])
    cadd_ht = cadd_ht.transmute(locus=locus, alleles=alleles)

    cadd_union_ht = cadd_ht.head(0)
    for contigs in (range(1, 10), list(range(10, 23)) + ["X", "Y", "MT"]):
        contigs = ["chr%s" % contig for contig in contigs] if genome_version == "38" else contigs
        cadd_ht_subset = cadd_ht.filter(hl.array(list(map(str, contigs))).contains(cadd_ht.locus.contig))
        cadd_union_ht = cadd_union_ht.union(cadd_ht_subset)

    cadd_union_ht = cadd_union_ht.key_by("locus", "alleles")

    cadd_union_ht.describe()

    return cadd_union_ht

for genome_version in ["37", "38"]:
    snvs_ht = import_cadd_table(f"gs://seqr-reference-data/GRCh{genome_version}/CADD/whole_genome_SNVs.v1.4.tsv.bgz", genome_version)
    indel_ht = import_cadd_table(f"gs://seqr-reference-data/GRCh{genome_version}/CADD/InDels.v1.4.tsv.bgz", genome_version)

    ht = snvs_ht.union(indel_ht)

    write_ht(ht, f"gs://seqr-reference-data/GRCh{genome_version}/CADD/CADD_snvs_and_indels_test.v1.4.ht")
