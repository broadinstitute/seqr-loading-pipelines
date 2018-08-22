import argparse
from pprint import pprint

from hail_scripts.v01.utils.hail_utils import create_hail_context

hc = create_hail_context()

p = argparse.ArgumentParser()
p.add_argument("input_path", nargs="+", help="input VCF or VDS")

args = p.parse_args()

for input_path in args.input_path:
    input_path = input_path.rstrip("/")
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

    #exome_calling_intervals_path = "gs://seqr-reference-data/GRCh37/exome_calling_regions.v1.interval_list"
    #exome_intervals = hail.KeyTable.import_interval_list(exome_calling_intervals_path)
    #vds = vds.filter_variants_table(exome_intervals, keep=False)

    print("\n==> sample_ids: " + "\t".join(["%s: %s" % (i, sample_id) for i, sample_id in enumerate(vds.sample_ids)]))
