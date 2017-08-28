import argparse
import hail
from pprint import pprint
from utils.computed_fields_utils import CONSEQUENCE_TERM_RANKS, CONSEQUENCE_TERMS

hc = hail.HailContext()

p = argparse.ArgumentParser()
p.add_argument("-s", "--schema-only", help="only print the schema, and skip running summarize()", action="store_true")
p.add_argument("input_path", help="input VCF or VDS")

args = p.parse_args()
input_path = args.input_path

print("Input path: %s" % input_path)

if input_path.endswith(".vds"):
    vds = hc.read(input_path)
else:
    vds = hc.import_vcf(input_path, min_partitions=1000, force_bgz=True)

#   485861 variants if keep=True
# 53723222 total variants [Stage 4:===================================================>(9999 + 1) / 10000]Summary(samples=899, variants=53723222, call_rate=0.972415, contigs=['X', '12', '8', '19', '4', '15', '11', '9', '22', '13', '16', '5', '10', '21', '6', '1', '17', '14', '20', '2', '18', '7', '3'], multiallelics=0, snps=43591287, mnps=0, insertions=4877287, deletions=5254528, complex=120, star=0, max_alleles=2)

print("\n==> sample schema: ")
pprint(vds.sample_schema)
print("\n==> variant schema: ")
pprint(vds.variant_schema)
print("\n==> genotype_schema: ")
pprint(vds.genotype_schema)

#exome_calling_intervals_path = "gs://seqr-reference-data/GRCh37/exome_calling_regions.v1.interval_list"
#exome_intervals = hail.KeyTable.import_interval_list(exome_calling_intervals_path)
#vds = vds.filter_variants_table(exome_intervals, keep=False)

print("\n==> sample_ids: " + "\t".join(["%s: %s" % (i, sample_id) for i, sample_id in enumerate(vds.sample_ids)]))

if not args.schema_only:
    print("==================")
    print(vds.summarize())
    print("==================")

