import argparse
import hail
from pprint import pprint

hc = hail.HailContext()

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

#   485861 variants if keep=True
# 53723222 total variants [Stage 4:===================================================>(9999 + 1) / 10000]Summary(samples=899, variants=53723222, call_rate=0.972415, contigs=['X', '12', '8', '19', '4', '15', '11', '9', '22', '13', '16', '5', '10', '21', '6', '1', '17', '14', '20', '2', '18', '7', '3'], multiallelics=0, snps=43591287, mnps=0, insertions=4877287, deletions=5254528, complex=120, star=0, max_alleles=2)

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
