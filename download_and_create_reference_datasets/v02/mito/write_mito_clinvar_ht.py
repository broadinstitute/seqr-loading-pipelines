import hail as hl

from download_and_create_reference_datasets.v02.mito.utils import load
from hail_scripts.utils.clinvar import CLINVAR_GOLD_STARS_LOOKUP

CONFIG = {
    'input_path': 'http://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz',
    'input_type': 'vcf',
    'output_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/clinvar/clinvar.GRCh38.chrM.ht',
    'annotate': {
        'allele_id': lambda ht: ht.info.ALLELEID,
        'clinical_significance': lambda ht: hl.delimit(ht.info.CLNSIG),
        'gold_stars': lambda ht: CLINVAR_GOLD_STARS_LOOKUP.get(hl.delimit(ht.info.CLNREVSTAT)),
    },
}


if __name__ == "__main__":
    load(CONFIG)
