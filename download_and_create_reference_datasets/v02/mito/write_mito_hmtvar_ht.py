import hail as hl

from download_and_create_reference_datasets.v02.mito.utils import load

CONFIG = {
    # The data source has certificate expiration issue. Manually downloaded from https://www.hmtvar.uniba.it/api/main/
    'input_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/HmtVar/HmtVar Jan. 10 2022.json',
    'input_type': 'json',
    'output_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/HmtVar/HmtVar Jan. 10 2022.ht',
    'annotate': {
        'locus': lambda ht: hl.locus('chrM', hl.parse_int32(ht.nt_start)),
        'alleles': lambda ht: [ht.ref_rCRS, ht.alt],
        'disease_score': lambda ht: hl.parse_float(ht.disease_score),
    },
}


if __name__ == "__main__":
    load(CONFIG)
