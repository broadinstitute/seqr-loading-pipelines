import argparse
import hail
from pprint import pprint

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

print("\n==> sample schema: ")
pprint(vds.sample_schema)
print("\n==> variant schema: ")
pprint(vds.variant_schema)
print("\n==> genotype_schema: ")
pprint(vds.genotype_schema)

print("\n==> sample_ids: " + "\t".join(["%s: %s" % (i, sample_id) for i, sample_id in enumerate(vds.sample_ids)]))

if not args.schema_only:
    print("==================")
    print(vds.summarize())
    print("==================")
