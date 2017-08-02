#!/usr/bin/env python

import argparse as ap
import hail
from pprint import pprint 

p = ap.ArgumentParser()
p.add_argument("dataset_path")
args = p.parse_args()

print(args.dataset_path)

hc = hail.HailContext(log="/hail.log")

print("==> import_dataset: %s" % args.dataset_path)
if args.dataset_path.endswith(".vds"):
    vds = hc.read(args.dataset_path)
else:
    vds = hc.import_vcf(args.dataset_path, force_bgz=True, min_partitions=10000)

print("==> summary: ")
s = vds.summarize()
print("")
pprint(s)

vds = vds.filter_intervals(hail.Interval.parse('chrX:31224000-31228000'))

print("==> split_multi")
vds = vds.split_multi()
print("")
pprint(vds.variant_schema)
print("==> count: %s" % str(vds.count()))

output_path = args.dataset_path.replace(".vcf", "").replace(".bgz", "").replace(".gz", "") + ".subset.vds"

print("==> output: %s" % output_path)
vds.write(output_path, overwrite=True)


