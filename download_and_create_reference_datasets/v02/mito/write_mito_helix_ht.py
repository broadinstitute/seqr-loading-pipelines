import hail as hl

from download_and_create_reference_datasets.v02.mito.utils import load

CONFIG = {
    'input_path': 'https://helix-research-public.s3.amazonaws.com/mito/HelixMTdb_20200327.tsv',
    'input_type': 'tsv',
    'output_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/Helix/HelixMTdb_20200327.ht',
    'field_types': {'counts_hom': hl.tint32, 'AF_hom': hl.tfloat64, 'counts_het': hl.tint32,
                    'AF_het': hl.tfloat64, 'max_ARF': hl.tfloat64, 'alleles': hl.tarray(hl.tstr)},
    'annotate': {
        'locus': lambda ht: hl.locus('chrM', hl.parse_int32(ht.locus.split(':')[1])),
        'AN': lambda ht: hl.if_else(ht.AF_hom > 0, hl.int32(ht.counts_hom/ht.AF_hom), hl.int32(ht.counts_het/ht.AF_het))
    },
}


if __name__ == "__main__":
    load(CONFIG)
