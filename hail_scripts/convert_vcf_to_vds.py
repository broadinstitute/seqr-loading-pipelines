#!/usr/bin/env python

import argparse as ap
import hail
from pprint import pprint

from hail_scripts.utils.computed_fields import get_expr_for_orig_alt_alleles_set

p = ap.ArgumentParser()
p.add_argument("--sites-only", action="store_true")
p.add_argument("vcf_path", nargs="+")
args = p.parse_args()

print(", ".join(args.vcf_path))

hc = hail.HailContext(log="/hail.log")

for vcf_path in args.vcf_path:
    print("\n")
    print("==> import_vcf: %s" % vcf_path)

    output_path = vcf_path.replace(".vcf", "").replace(".gz", "").replace(".bgz", "") + ".vds"
    print("==> output: %s" % output_path)

    if args.sites_only:
        vds = hc.import_vcf(vcf_path, force_bgz=True, min_partitions=10000, drop_samples=True)
    else:
        vds = hc.import_vcf(vcf_path, force_bgz=True, min_partitions=10000)

    print("\n==> split_multi")
    vds = vds.annotate_variants_expr("va.originalAltAlleles=%s" % get_expr_for_orig_alt_alleles_set())
    if vds.was_split():
        vds = vds.annotate_variants_expr('va.aIndex = 1, va.wasSplit = false')
    else:
        vds = vds.split_multi()


    print("")
    pprint(vds.variant_schema)

    vds.write(output_path, overwrite=True)
