import argparse
import hail

p = argparse.ArgumentParser()
p.add_argument('vcf', help='Input vcf gs:// path')
options = p.parse_args()

if not options.vcf.startswith('gs://'):
    p.error('Google bucket path must start with gs://: %s' % options.vcf)


output_vds = options.vcf.split('.vcf')[0] + '.vds'
output_vcf = options.vcf.replace('.vcf', '.vep.vcf')
assert output_vcf != options.vcf

hc = hail.HailContext()

vds = (
hc.import_vcf(options.vcf, npartitions=10000)
 .split_multi()
 .annotate_variants_expr('va.info.AC = va.info.AC[va.aIndex - 1], va.info.AF = va.info.AF[va.aIndex - 1]')
 .vep('/vep/vep-gcloud.properties', root='va.info.CSQ', block_size=100, force=True, csq=True)
 .write(output_vds, overwrite=True)
)

print(vds)
#from pprint import pprint
#print("SCHEMA: ")
#pprint(dict([(f.name, f.typ) for f in vds.variant_schema.fields]))
#print("COUNT", vds.count())

#vds.export_vcf(output_vcf)
