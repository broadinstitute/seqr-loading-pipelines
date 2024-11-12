from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_data.cadd import load_cadd_ht_from_raw_dataset
from v03_pipeline.lib.reference_data_OLD.hgmd import download_and_import_hgmd_vcf

CONFIG = {
    'cadd': {
        'load_parsed_dataset_func': load_cadd_ht_from_raw_dataset,
        ReferenceGenome.GRCh37: {
            'version': '0.0',
            'raw_dataset_path': [
                'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh37/whole_genome_SNVs.tsv.gz',
                'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh38/gnomad.genomes.r4.0.indel.tsv.gz',
            ],
        },
        ReferenceGenome.GRCh38: {
            'version': '0.0',
            'raw_dataset_path': [
                'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh38/whole_genome_SNVs.tsv.gz',
                'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh37/gnomad.genomes-exomes.r4.0.indel.tsv.gz',
            ],
        },
    },
    'hgmd': {
        'load_parsed_dataset_func': download_and_import_hgmd_vcf,
        ReferenceGenome.GRCh37: {
            'version': '0.0',
            'raw_dataset_path': 'gs://seqr-reference-data/v3.1/GRCh37/reference_datasets/hgmd-v2020.1.vcf.bgz',
        },
        ReferenceGenome.GRCh38: {
            'version': '0.0',
            'raw_dataset_path': 'gs://seqr-reference-data/v3.1/GRCh38/reference_datasets/hgmd-v2020.1.vcf.bgz',
        },
    },
}
