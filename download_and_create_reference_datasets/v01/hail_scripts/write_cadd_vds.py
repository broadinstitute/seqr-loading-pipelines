# script for combining downloaded CADD SNP and Indel vcfs into 1 vds for each genome build.

import hail
import os
import time

from hail_scripts.v01.utils.vds_utils import write_vds

for cadd_snvs_path, cadd_indels_path in [
        (
                'gs://seqr-reference-data/GRCh37/CADD/whole_genome_SNVs.v1.4.vcf.gz',
                'gs://seqr-reference-data/GRCh37/CADD/InDels.v1.4.vcf.gz'
        ),
        (
                'gs://seqr-reference-data/GRCh38/CADD/whole_genome_SNVs.v1.4.vcf.gz',
                'gs://seqr-reference-data/GRCh38/CADD/InDels.v1.4.vcf.gz'
        )
    ]:

    hail_context = hail.HailContext(log="./hail_{}.log".format(time.strftime("%y%m%d_%H%M%S")))

    print("==> reading in CADD: %s, %s" % (cadd_snvs_path, cadd_indels_path))

    vds = hail_context.import_vcf([cadd_snvs_path, cadd_indels_path], force_bgz=True, min_partitions=10000)

    vds = vds.split_multi()
    vds = vds.filter_intervals(hail.Interval.parse("1-MT"))

    vds = vds.persist()

    output_path = os.path.join(os.path.dirname(cadd_indels_path), "CADD_snvs_and_indels.v1.4.vds")

    write_vds(vds, output_path)

    hail_context.stop()
