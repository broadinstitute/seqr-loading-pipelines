import argparse as ap
from pprint import pprint

from hail_scripts.v01.utils.computed_fields import get_expr_for_orig_alt_alleles_set
from hail_scripts.v01.utils.hail_utils import create_hail_context
from hail_scripts.v01.utils.vds_utils import write_vds

p = ap.ArgumentParser()
p.add_argument("--sites-only", help="Ignore all genotype info in the vcf.", action="store_true")
p.add_argument("--version", help="(optional) version string to put in VDS globals.version")
p.add_argument("--output-vds", help="(optional) output vds path")
p.add_argument("vcf_path", nargs="+")
args = p.parse_args()

print(", ".join(args.vcf_path))

hc = create_hail_context()

vcf_path = ",".join(args.vcf_path)
print("\n")
print("==> import_vcf: %s" % vcf_path)

if args.output_vds:
    output_path = args.output_vds
else:
    output_path = vcf_path.replace(".vcf", "").replace(".gz", "").replace(".bgz", "").replace(".*", "").replace("*", "")+".vds"

print("==> output: %s" % output_path)

if args.sites_only:
    vds = hc.import_vcf(vcf_path, force_bgz=True, min_partitions=10000, drop_samples=True)
else:
    vds = hc.import_vcf(vcf_path, force_bgz=True, min_partitions=10000)

vds = vds.annotate_global_expr('global.sourceFilePath = "{}"'.format(vcf_path))
if args.version:
    vds = vds.annotate_global_expr('global.version = "{}"'.format(args.version))

print("\n==> split_multi")
vds = vds.annotate_variants_expr("va.originalAltAlleles=%s" % get_expr_for_orig_alt_alleles_set())

# ensure that va.wasSplit and va.aIndex are defined before calling split_multi() since split_multi() doesn't define these if all variants are bi-allelic
vds = vds.annotate_variants_expr('va.wasSplit=false, va.aIndex=1')
vds = vds.split_multi()

print("")
pprint(vds.variant_schema)

write_vds(vds, output_path)
