import argparse
import hail
import logging
import pprint

from hail_scripts.v01.utils.computed_fields import get_expr_for_orig_alt_alleles_set
from hail_scripts.v01.utils.hail_utils import create_hail_context
from hail_scripts.v01.utils.vds_utils import write_vds, run_vep

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

p = argparse.ArgumentParser()
p.add_argument("--genome-version", help="Genome build: 37 or 38", choices=["37", "38"], required=True)
p.add_argument("--block-size", help="batch size - how many variants to pass to VEP for each VEP run", type=int, default=500)
p.add_argument('--subset', const="X:31097677-33339441", nargs='?',
               help="subset to this chrom:start-end range. Intended for testing.")
p.add_argument("input_file", help="input vcf or vds")
p.add_argument("output_vds", nargs="?", help="output vds")
args = p.parse_args()

input_path = args.input_file.rstrip("/")
print("Input File: %s" % (input_path, ))
if not args.output_vds:
    output_vds_prefix = input_path.replace(".vcf", "").replace(".vds", "").replace(".bgz", "").replace(".gz", "").replace(".vep", "")
    args.output_vds = output_vds_prefix + ".vep.vds"

print("Output VDS: %s" % (args.output_vds, ))

hc = create_hail_context()
if input_path.endswith(".vds"):
    vds = hc.read(input_path)
elif input_path.endswith(".vcf") or input_path.endswith("gz"):
    vds = hc.import_vcf(input_path, force_bgz=True, min_partitions=10000)

    # ensure that va.wasSplit and va.aIndex are defined before calling split_multi() since split_multi() doesn't define these if all variants are bi-allelic
    vds = vds.annotate_variants_expr('va.wasSplit=false, va.aIndex=1')
else:
    p.error("Invalid input file: %s" % input_path)

if vds.num_partitions() < 50:
    print("Repartitioning")
    vds = vds.repartition(10000)

vds = vds.annotate_variants_expr("va.originalAltAlleles=%s" % get_expr_for_orig_alt_alleles_set()) # save alt alleles before calling split_multi
vds = vds.split_multi()

#vds = vds.filter_alleles('v.altAlleles[aIndex-1].isStar()', keep=False)
filter_interval = "1-MT"
if args.subset:
    filter_interval = args.subset

logger.info("\n==> set filter interval to: %s" % (filter_interval, ))
vds = vds.filter_intervals(hail.Interval.parse(filter_interval))

summary = vds.summarize()
pprint.pprint(summary)
if summary.variants == 0:
    p.error("0 variants in VDS. Make sure chromosome names don't contain 'chr'")

vds = run_vep(vds, genome_version=args.genome_version, block_size=args.block_size)

write_vds(vds, args.output_vds)

pprint.pprint(vds.variant_schema)
