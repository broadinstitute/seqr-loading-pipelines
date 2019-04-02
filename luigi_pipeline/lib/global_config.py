import luigi

class GlobalConfig(luigi.Config):
    validation_37_noncoding_ht = luigi.Parameter(
        default='gs://seqr-reference-data/GRCh37/validate_ht/common_noncoding_variants.grch37.ht'
    )
    validation_37_coding_ht = luigi.Parameter(
        default='gs://seqr-reference-data/GRCh37/validate_ht/common_coding_variants.grch37.ht'
    )
    validation_38_noncoding_ht = luigi.Parameter(
        default='gs://seqr-reference-data/GRCh38/validate_ht/common_noncoding_variants.grch38.ht'
    )
    validation_38_coding_ht = luigi.Parameter(
        default='gs://seqr-reference-data/GRCh38/validate_ht/common_coding_variants.grch38.ht'
    )
