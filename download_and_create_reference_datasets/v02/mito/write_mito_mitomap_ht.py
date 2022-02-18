import hail as hl

from download_and_create_reference_datasets.v02.mito.utils import load

CONFIG = {
    # The data source is a web page. So we download it manually and save them to the GCS.
    'input_path': 'https://storage.googleapis.com/seqr-reference-data/GRCh38/mitochondrial/MITOMAP/Mitomap%20Confirmed%20Mutations%20Feb.%2004%202022.tsv',
    'input_type': 'tsv',
    'output_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/MITOMAP/Mitomap Confirmed Mutations Feb. 04 2022.ht',
    'annotate': {
        'locus': lambda ht: hl.locus('chrM', hl.parse_int32(ht.Allele.first_match_in('m.([0-9]+)')[0])),
        'alleles': lambda ht: ht.Allele.first_match_in('m.[0-9]+([ATGC]+)>([ATGC]+)'),
        'pathogenic': lambda ht: hl.is_defined(ht['Associated Diseases'])
    },
}


if __name__ == "__main__":
    load(CONFIG)
