import argparse
from pprint import pprint

from hail_scripts.v01.utils.hail_utils import create_hail_context

hc = create_hail_context()

p = argparse.ArgumentParser()
p.add_argument("-s", "--schema-only", help="only print the schema, and skip running summarize()", action="store_true")
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

    print("\n==> sample_ids: " + "\t".join(["%s: %s" % (i, sample_id) for i, sample_id in enumerate(vds.sample_ids)]))

    kt = vds.make_table("v=v, va=va", []).to_dataframe().show(n=50)

    if not args.schema_only:
        print("==================")
        print("Total - before split_multi()")
        print(vds.summarize())

        print("==================")
        vds = vds.split_multi()

        print("Total - after split_multi()")
        print(vds.summarize())

        print("==================")
        vds = vds.impute_sex(maf_threshold=0.01)
        print("Inferred Sex - computed by hail vds.impute_sex(maf_threshold=0.01)")
        print("\t".join(["F stat", "Sex", "Sample Id"]))
        for sample_id, annotations in sorted(vds.sample_annotations.items(), key=lambda i: i[0]):
            fstat = annotations["imputesex"]["Fstat"]
            if fstat is None:
                fstat = float('NaN')
            is_female = annotations["imputesex"]["isFemale"]
            if is_female is None:
                sex = '?'
            elif is_female is True:
                sex = 'F'
            else:
                sex = 'M'

            print("%0.3f\t%s\t%s" % (fstat, sex, sample_id))
