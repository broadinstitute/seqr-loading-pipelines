import argparse as ap
import logging
from hail_scripts.v02.utils.hail_utils import write_mt, write_ht, import_vcf

logger = logging.getLogger()

p = ap.ArgumentParser()
p.add_argument("--genome-version", help="genome version", choices=["37", "38"], required=True)
p.add_argument("--version", help="(optional) version string to add as a global annotation in the MT: globals.version")

g = p.add_mutually_exclusive_group(required=True)
g.add_argument("-ht", "--output-sites-only-ht", help="output a sites-only hail table", action="store_true")
g.add_argument("-mt", "--output-mt", help="output a matrix table", action="store_true")

p.add_argument("--output", help="(optional) output path")
p.add_argument("vcf_path", nargs="+")
args = p.parse_args()

logger.info(", ".join(args.vcf_path))

vcf_path = ",".join(args.vcf_path)
logger.info("\n")
logger.info("==> import_vcf: %s" % vcf_path)

if args.output:
    output_path = args.output
else:
    output_path = vcf_path.replace(".vcf", "").replace(".gz", "").replace(".bgz", "").replace(".*", "").replace("*", "")+(
        ".mt" if args.output_mt else ".ht")

if args.output_mt and not output_path.endswith(".mt"):
    p.error(f"output path doesn't end with .mt: {output_path}")
if args.output_sites_only_ht and not output_path.endswith(".ht"):
    p.error(f"output path doesn't end with .ht: {output_path}")

logger.info("==> output: %s" % output_path)

mt = import_vcf(vcf_path, args.genome_version, force_bgz=True, min_partitions=10000, drop_samples=True)

if args.version:
    mt = mt.annotate_globals(version=args.version)

ht = mt.rows()
ht.show(5)

if args.output_sites_only_ht:
    ht.describe()
    write_ht(ht, output_path)
else:
    mt.describe()
    write_mt(mt, output_path)
