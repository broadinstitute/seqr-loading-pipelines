import argparse
import hail
import pprint

from utils.computed_fields_utils import get_expr_for_orig_alt_alleles_set

p = argparse.ArgumentParser()
p.add_argument("input_file", help="input vcf or vds")
p.add_argument("output_vds", nargs="?", help="output vds")
args = p.parse_args()

print("Input File: %s" % (args.input_file, ))
if not args.output_vds:
    output_vds_prefix = args.input_file.replace(".vcf", "").replace(".vds", "").replace(".bgz", "").replace(".gz", "").replace(".vep", "")
    args.output_vds = output_vds_prefix + ".vep.vds"

print("Output VDS: %s" % (args.output_vds, ))

hc = hail.HailContext(log="/hail.log")
if args.input_file.endswith(".vds"):
    vds = hc.read(args.input_file)
elif args.input_file.endswith("gz"):
    vds = hc.import_vcf(args.input_file, force_bgz=True, min_partitions=10000)
    # save alt alleles before calling split_multi
else:
    p.error("Invalid input file: %s" % args.input_file)

vds = vds.annotate_variants_expr("va.originalAltAlleles=%s" % get_expr_for_orig_alt_alleles_set())
vds = vds.split_multi().vep(config="/vep/vep-gcloud.properties", root='va.vep', block_size=500)

pprint.pprint(vds.variant_schema)

vds.write(args.output_vds, overwrite=True)
