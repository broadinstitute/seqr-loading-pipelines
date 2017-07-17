import sys
import hail
from testdata import *

vcf = sys.argv[1]

hc = hail.HailContext(master='local[4]')

vds = (hc.import_vcf(vcf)
       .split_multi()
       .cache())

(vds.export_variants_solr(
    solr_condition(variant_fields, variants = True), solr_condition(genotype_fields(True)),
    'localhost:9983', 'test',
    export_missing = True))

(vds.export_variants_cass(
    export_condition(variant_fields), export_condition(genotype_fields()),
    address = 'localhost', keyspace = 'test', table = 'test',
    export_missing = True, export_ref = True,
    block_size = 5, rate = 5000 / 5 / 4))

vds.write('/data/seqr/datasets/test.vds', overwrite = True)
