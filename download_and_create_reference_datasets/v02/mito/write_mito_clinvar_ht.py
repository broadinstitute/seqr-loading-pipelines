import hail as hl

from download_and_create_reference_datasets.v02.mito.utils import load

CONFIG = {
    'input_path': 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz',
    'input_type': 'vcf',
    'output_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/clinvar/clinvar.GRCh38.chrM.ht',
    'annotate': {
        'ALLELEID': lambda ht: ht.info.ALLELEID,
        'CLNSIG': lambda ht: ht.info.CLNSIG,
        'CLNREVSTAT': lambda ht: ht.info.CLNREVSTAT,
    },
}


if __name__ == "__main__":
    load(CONFIG)
