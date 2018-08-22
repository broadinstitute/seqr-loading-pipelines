import argparse
from pprint import pprint

from hail_scripts.v01.utils.hail_utils import create_hail_context

hc = create_hail_context()

p = argparse.ArgumentParser()
p.add_argument("--no-header", action="store_true", help="table doesn't have a header")
p.add_argument("--delimiter", default="\t", help="column delimiter")
p.add_argument("--missing-value", default="NA", help="value that should be interpreted as missing")
p.add_argument("input_path", help="input VCF or VDS")

args = p.parse_args()
input_path = args.input_path

print("Input path: %s" % input_path)

kt = hc.import_table(input_path, impute=True, no_header=args.no_header, delimiter=args.delimiter, missing=args.missing_value)

print("\n==> keytable has %s columns: %s" % (kt.num_columns, kt.columns))

print("\n==> keytable schema: ")
pprint(kt.schema)

print("\n==> count: ")
pprint(kt.count())
