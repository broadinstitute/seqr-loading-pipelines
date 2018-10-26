import argparse as ap
import hail
from pprint import pformat
import time

p = ap.ArgumentParser(description="Convert a tsv table to a .kt")
p.add_argument("--no-header", action="store_true", help="specifies that tsv table doesn't have a header")
p.add_argument("--key-by", action="append", help="column name(s) to use as the key")
p.add_argument("tsv_path")

args = p.parse_args()

print("Input path: " + str(args.tsv_path))

hc = hail.HailContext(log="./hail_{}.log".format(time.strftime("%y%m%d_%H%M%S")))

print("==> import_table: %s" % args.tsv_path)

output_path = args.tsv_path.replace(".tsv", "").replace(".gz", "").replace(".bgz", "") + ".kt"
print("==> output: %s" % output_path)

kt = hc.import_table(args.tsv_path, impute=True, no_header=args.no_header).repartition(500)

if args.key_by:
    print("==> key by: " + str(args.key_by))
    kt = kt.key_by(args.key_by)

print("==> writing out key table: %s\n%s" % (output_path, pformat(kt.schema)))
kt.write(output_path, overwrite=True)
