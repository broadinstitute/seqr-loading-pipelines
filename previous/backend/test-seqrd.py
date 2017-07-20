import hail
from hail.java import scala_package_object
from testdata import *

hc = hail.HailContext(master='local[4]')

seqr = scala_package_object(hc.hail.seqr)

seqr.test(
    hc.jsql_context,
    seqr.cloudSolrClient('localhost:9983', 'test',),
    export_condition(variant_fields), export_condition(genotype_fields()),
    '/data/seqr/datasets/test.vds')
