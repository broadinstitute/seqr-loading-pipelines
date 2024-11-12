import tempfile
import urllib

import hail as hl

from v03_pipeline.lib.model import ReferenceGenome


def key_by_locus_alleles(ht: hl.Table, reference_genome: ReferenceGenome) -> hl.Table:
    chrom = (
        hl.format('chr%s', ht.chrom)
        if reference_genome == ReferenceGenome.GRCh38
        else ht.chrom
    )
    locus = hl.locus(chrom, ht.pos, reference_genome.value)
    alleles = hl.array([ht.ref, ht.alt])
    ht = ht.transmute(locus=locus, alleles=alleles)
    return ht.key_by('locus', 'alleles')
