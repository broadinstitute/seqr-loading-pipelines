import argparse
from pprint import pprint

from hail_scripts.v01.utils.hail_utils import create_hail_context

hc = create_hail_context()

p = argparse.ArgumentParser()
p.add_argument("--all", action="store_true")
p.add_argument("-n", "--num-variants", help="number of variants to export", default=500, type=int)
p.add_argument("input_path", help="input VCF or VDS")


args = p.parse_args()
input_path = args.input_path.rstrip("/")

print("Input path: %s" % input_path)

if input_path.endswith(".vds"):
    vds = hc.read(input_path)
    if vds.num_partitions() < 50:
        vds = vds.repartition(1000)
else:
    vds = hc.import_vcf(input_path, min_partitions=1000, force_bgz=True)

print("\n==> variant schema: ")
pprint(vds.variant_schema)


#exome_calling_intervals_path = "gs://seqr-reference-data/GRCh37/exome_calling_regions.v1.interval_list"
#exome_intervals = hail.KeyTable.import_interval_list(exome_calling_intervals_path)
#vds = vds.filter_variants_table(exome_intervals, keep=False)

print("\n==> sample_ids: " + "\t".join(["%s: %s" % (i, sample_id) for i, sample_id in enumerate(vds.sample_ids)]))

total_variants = vds.count()

if not args.all:
    print("\n==> count: %s" % (total_variants,))
    vds = vds.sample_variants(args.num_variants / float(total_variants[1]), seed=1)

input_path_prefix = input_path.replace(".vds", "").replace(".vcf", "").replace(".gz", "").replace(".bgz", "")
print("\n==> writing out: " + input_path_prefix + ".tsv")

expr = "v = v, va = va.*" # , " + ", ".join(["%s = gs.collect()[%d]" % (sample_id, i) for i, sample_id in enumerate(vds.sample_ids)])
print("\n==> " + expr)
vds.export_variants(input_path_prefix + ".tsv", expr)
