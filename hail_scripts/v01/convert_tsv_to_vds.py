import argparse as ap
import hail
from pprint import pprint
import time

from hail_scripts.v01.utils.vds_utils import write_vds

p = ap.ArgumentParser(description="Convert a tsv table to a .vds")
p.add_argument("-c", "--chrom-column", required=True)
p.add_argument("-p", "--pos-column", required=True)
p.add_argument("-r", "--ref-column", required=True)
p.add_argument("-a", "--alt-column", required=True)

p.add_argument("table_path", nargs="+")
args = p.parse_args()

print(", ".join(args.vcf_path))

hc = hail.HailContext(log="./hail_{}.log".format(time.strftime("%y%m%d_%H%M%S")))

for table_path in args.table_path:
    print("\n")
    print("==> import_table: %s" % table_path)

    output_path = table_path.replace(".tsv", "").replace(".gz", "").replace(".bgz", "") + ".vds"
    print("==> output: %s" % output_path)

    kt = hc.import_table(table_path, impute=True, no_header=args.no_header, delimiter=args.delimiter, missing=args.missing_value, min_partitions=1000)

    #kt = kt.drop(columns_to_drop)
    #kt = kt.rename(rename_columns)
    kt = kt.filter("%(ref_column)s == %(alt_column)s" % args.__dict__, keep=False)
    kt = kt.annotate("variant=Variant(%(chrom_column)s, %(pos_column)s, %(ref_column)s, %(alt_column)s)" % args.__dict__)
    kt = kt.key_by('variant')
    kt = kt.drop([args.chrom_column, args.pos_column, args.ref_column, args.alt_column])

    vds = hail.VariantDataset.from_table(kt)

    pprint(vds.variant_schema)
    write_vds(vds, output_path)
