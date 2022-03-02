import hail as hl

from download_and_create_reference_datasets.v02.mito.utils import load

CONFIG = {
    'input_path': 'https://mitimpact.css-mendel.it/cdn/MitImpact_db_3.0.7.txt.zip',
    'input_type': 'tsv',
    'output_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/MitImpact/MitImpact_db_3.0.7.ht',
    'annotate': {
        'locus': lambda ht: hl.locus('chrM', hl.parse_int32(ht.Start)),
        'alleles': lambda ht: [ht.Ref, ht.Alt],
        'APOGEE_score': lambda ht: hl.parse_float(ht.APOGEE_score),
    },
}


if __name__ == "__main__":
    load(CONFIG)
