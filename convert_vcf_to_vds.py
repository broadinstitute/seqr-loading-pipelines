#!/usr/bin/env python

import argparse as ap
import hail
from pprint import pprint

from utils.computed_fields_utils import get_expr_for_orig_alt_alleles_set

p = ap.ArgumentParser()
p.add_argument("vcf_path", nargs="+")
args = p.parse_args()

print(", ".join(args.vcf_path))

hc = hail.HailContext(log="/hail.log")

for vcf_path in args.vcf_path:
    print("\n")
    print("==> import_vcf: %s" % vcf_path)

    output_path = vcf_path.replace(".vcf", "").replace(".gz", "") + ".vds"
    print("==> output: %s" % output_path)

    vds = hc.import_vcf(vcf_path, force_bgz=True, min_partitions=10000)


    #vds = hc.read("gs://seqr-reference-data/GRCh38/CADD/whole_genome_SNVs.liftover.GRCh38.vds")
    #output_path = "gs://seqr-reference-data/GRCh38/CADD/whole_genome_SNVs.liftover.GRCh38.fixed.vds"

    #vds = hc.read("gs://seqr-reference-data/GRCh38/CADD/InDels.liftover.GRCh38.vds")
    #output_path = "gs://seqr-reference-data/GRCh38/CADD/InDels.liftover.GRCh38.fixed.vds"

    print("\n==> split_multi")
    vds = vds.annotate_variants_expr("va.originalAltAlleles=%s" % get_expr_for_orig_alt_alleles_set())
    vds = vds.split_multi()
    print("")
    pprint(vds.variant_schema)
    

    vds.write(output_path, overwrite=True)
