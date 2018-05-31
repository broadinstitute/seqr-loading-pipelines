# script for combining downloaded CADD SNP and Indel vcfs into 1 vds for each genome build. 

import hail
import os

for cadd_snvs_path, cadd_indels_path in [
        (
                'gs://seqr-reference-data/GRCh37/CADD/whole_genome_SNVs.vcf.gz',
                'gs://seqr-reference-data/GRCh37/CADD/InDels.vcf.gz'
        ),
        (
                'gs://seqr-reference-data/GRCh38/CADD/whole_genome_SNVs.liftover.GRCh38.vcf.gz',
                'gs://seqr-reference-data/GRCh38/CADD/InDels.liftover.GRCh38.vcf.gz'
        )
    ]:

    hail_context = hail.HailContext()

    print("==> Reading in CADD: %s, %s" % (cadd_snvs_path, cadd_indels_path))

    vds = hail_context.import_vcf([cadd_snvs_path, cadd_indels_path], force_bgz=True, min_partitions=10000)

    vds = vds.split_multi()
    vds = vds.filter_intervals(hail.Interval.parse("1-MT"))

    vds = vds.persist()

    output_path = os.path.join(os.path.dirname(cadd_indels_path), "CADD_snvs_and_indels.vds")

    print("==> Writing out " + output_path)

    vds.write(output_path, overwrite=True)

    hail_context.stop()
