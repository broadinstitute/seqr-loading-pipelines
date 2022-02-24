import hail as hl

from download_and_create_reference_datasets.v02.mito.utils import load

CONFIG = {
    'input_path': 'https://www.hmtvar.uniba.it/api/main/',
    'input_type': 'json',
    'skip_verify_ssl': True,  # The certificate of the website has expired.
    'output_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/HmtVar/HmtVar Jan. 10 2022.ht',
    'annotate': {
        'locus': lambda ht: hl.locus('chrM', hl.parse_int32(ht.nt_start)),
        'alleles': lambda ht: [ht.ref_rCRS, ht.alt],
        'disease_score': lambda ht: hl.parse_float(ht.disease_score),
    },
}


if __name__ == "__main__":
    load(CONFIG)
