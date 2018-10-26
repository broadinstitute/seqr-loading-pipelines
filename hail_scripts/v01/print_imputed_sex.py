import argparse
import hail
import time

hc = hail.HailContext(log="./hail_{}.log".format(time.strftime("%y%m%d_%H%M%S")))

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

    vds = vds.split_multi().impute_sex(maf_threshold=0.01)

    print("\n==> sample_ids: " + "\t".join(["%s: %s" % (i, sample_id) for i, sample_id in enumerate(vds.sample_ids)]))
    print("\n==> sample annotations: ")

    print("\t".join(["sample_id", "sex", "FStat"]))
    for sample_id, annotations in vds.sample_annotations.items():
        print("%s\t%s\t%0.3f" % (
            sample_id,
            "female" if annotations["imputesex"]["isFemale"] else "male",
            annotations["imputesex"]["Fstat"]
        ))
