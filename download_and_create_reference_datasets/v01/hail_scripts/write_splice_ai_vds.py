# script for combining the precomputed splice_ai exome and genome vcfs into 1 vds for each genome build.

import hail
import os
import time

from hail_scripts.v01.utils.vds_utils import write_vds

for splice_ai_snvs_path, splice_ai_indels_path in [
    (
            'gs://seqr-reference-data/GRCh37/spliceai/exome_spliceai_scores.vcf.gz',
            'gs://seqr-reference-data/GRCh37/spliceai/whole_genome_filtered_spliceai_scores.vcf.gz'
    ),
    (
            'gs://seqr-reference-data/GRCh38/spliceai/exome_spliceai_scores.liftover.vcf.gz',
            'gs://seqr-reference-data/GRCh38/spliceai/whole_genome_filtered_spliceai_scores.liftover.vcf.gz'
    )
]:

    hail_context = hail.HailContext(log="./hail_{}.log".format(time.strftime("%y%m%d_%H%M%S")))

    print("==> reading in splice_ai vcfs: %s, %s" % (splice_ai_snvs_path, splice_ai_indels_path))

    vds = hail_context.import_vcf([splice_ai_snvs_path, splice_ai_indels_path], force_bgz=True, min_partitions=10000)

    #vds = vds.split_multi()
    vds = vds.filter_intervals(hail.Interval.parse("1-MT"))

    vds = vds.persist()

    vds = vds.annotate_variants_expr("va.info.max_DS = [va.info.DS_AG, va.info.DS_AL, va.info.DS_DG, va.info.DS_DL].max()")

    output_path = os.path.join(os.path.dirname(splice_ai_indels_path), "spliceai_scores.vds")

    write_vds(vds, output_path)

    hail_context.stop()
