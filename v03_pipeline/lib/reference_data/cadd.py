import tempfile
import urllib

import hail as hl

from v03_pipeline.lib.model import ReferenceGenome


# adapted from download_and_create_reference_datasets/v02/hail_scripts/write_cadd_ht.py
def import_cadd_table(
    raw_dataset_path: str,
    reference_genome: ReferenceGenome,
):
    column_names = {
        'f0': 'chrom',
        'f1': 'pos',
        'f2': 'ref',
        'f3': 'alt',
        'f4': 'RawScore',
        'f5': 'PHRED',
    }
    types = {'f0': hl.tstr, 'f1': hl.tint, 'f4': hl.tfloat32, 'f5': hl.tfloat32}

    with tempfile.NamedTemporaryFile(suffix='.tsv.gz', delete=False) as tmp_file:
        urllib.request.urlretrieve(raw_dataset_path, tmp_file.name)  # noqa: S310
        cadd_ht = hl.import_table(
            tmp_file,
            force_bgz=True,
            comment='#',
            no_header=True,
            types=types,
            min_partitions=10000,
        )
        cadd_ht = cadd_ht.rename(column_names)
        chrom = (
            hl.format('chr%s', cadd_ht.chrom)
            if reference_genome == ReferenceGenome.GRCh38
            else cadd_ht.chrom
        )
        locus = hl.locus(
            chrom,
            cadd_ht.pos,
            reference_genome=hl.get_reference(ReferenceGenome.GRCh38),
        )
        alleles = hl.array([cadd_ht.ref, cadd_ht.alt])
        cadd_ht = cadd_ht.transmute(locus=locus, alleles=alleles)

        cadd_union_ht = cadd_ht.head(0)
        contigs = reference_genome.standard_contigs.union(
            reference_genome.optional_contigs,
        )
        for contig_subset in (contigs[:9], contigs[9:]):
            cadd_ht_subset = cadd_ht.filter(
                hl.array(list(map(str, contig_subset))).contains(cadd_ht.locus.contig),
            )
            cadd_union_ht = cadd_union_ht.union(cadd_ht_subset)

        return cadd_union_ht.key_by('locus', 'alleles')


def load_cadd_ht_from_raw_dataset(
    raw_dataset_paths: dict[str, str],
    reference_genome: ReferenceGenome,
):
    snvs_ht = import_cadd_table(raw_dataset_paths['snv'], reference_genome)
    indel_ht = import_cadd_table(raw_dataset_paths['indel'], reference_genome)
    ht = snvs_ht.union(indel_ht)
    return ht.naive_coalesce(10000)
