import argparse as ap
import hail
from pprint import pprint
import time

from hail_scripts.v01.utils.computed_fields import get_expr_for_orig_alt_alleles_set
from hail_scripts.v01.utils.vds_utils import write_vds

p = ap.ArgumentParser()
p.add_argument("-g", "--genome-version", help="Genome build: 37 or 38", choices=["37", "38"], required=True)
p.add_argument("--chrom", help="subset to this chromosome", choices=map(str, range(1, 23)+["X", "Y"]))
p.add_argument("dataset_path")
args = p.parse_args()

hc = hail.HailContext(log="./hail_{}.log".format(time.strftime("%y%m%d_%H%M%S")))

print("\n==> input: %s" % args.dataset_path)

output_path = args.dataset_path.replace(".vcf", "").replace(".bgz", "").replace(".gz", "")
if args.chrom:
    output_path += ".chr%s_subset.vds" % str(args.chrom)
else:
    output_path += ".DMD_subset.vds"
print("\n==> output: %s" % output_path)

if args.dataset_path.endswith(".vds"):
    vds = hc.read(args.dataset_path)
else:
    vds = hc.import_vcf(args.dataset_path, force_bgz=True, min_partitions=10000)

if args.chrom:
    interval = hail.Interval.parse('%s:1-500000000' % str(args.chrom))
else:
    if args.genome_version == "37":
        interval = hail.Interval.parse('X:31224000-31228000')
    elif args.genome_version == "38":
        interval = hail.Interval.parse('X:31205883-31209883')
    else:
        p.error("Unexpected genome version: " + str(args.genome_version))

vds = vds.filter_intervals(interval)

print("\n==> split_multi")
vds = vds.annotate_variants_expr("va.originalAltAlleles=%s" % get_expr_for_orig_alt_alleles_set())
vds = vds.split_multi()
print("")
pprint(vds.variant_schema)

print("\n==> summary: %s" % str(vds.summarize()))

write_vds(vds, output_path)
