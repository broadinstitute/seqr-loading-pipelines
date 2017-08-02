#!/usr/bin/env python

import argparse as ap
import hail
from pprint import pprint 

p = ap.ArgumentParser()
p.add_argument("vcf_path")
args = p.parse_args()

print(args.vcf_path)

hc = hail.HailContext(log="/hail.log")

output_path = args.vcf_path.replace(".vcf", "").replace(".gz", "") + ".vds"

print("==> import_vcf: %s" % args.vcf_path)
vds = hc.import_vcf(args.vcf_path, force_bgz=True, min_partitions=10000)
print("==> split_multi")
vds = vds.split_multi()
print("")
pprint(vds.variant_schema)
print("==> output: %s" % output_path)
vds.write(output_path, overwrite=True)


