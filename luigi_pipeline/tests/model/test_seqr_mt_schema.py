import unittest

import hail as hl

from lib.model.seqr_mt_schema import SeqrVariantSchema
from tests.data.sample_vep import VEP_DATA, DERIVED_DATA

class TestSeqrModel(unittest.TestCase):

    def test_variant_derived_fields(self):
        rsid = 'rs35471880'

        mt = hl.import_vcf('tests/data/1kg_30variants.vcf.bgz')
        mt = hl.split_multi(mt.filter_rows(mt.rsid == rsid))
        mt = mt.annotate_rows(**VEP_DATA[rsid])

        seqr_schema = SeqrVariantSchema(mt)
        seqr_schema.sorted_transcript_consequences().doc_id(512).variant_id().contig().pos().start().end().ref().alt() \
            .pos().xstart().xstop().xpos().transcript_consequence_terms().transcript_ids().main_transcript().gene_ids() \
            .coding_gene_ids().domains().ac().af().an()
        mt = seqr_schema.select_annotated_mt()

        obj = mt.rows().collect()[0]

        # Cannot do a nested compare because of nested hail objects, so do one by one.
        fields = ['AC', 'AF', 'AN', 'codingGeneIds', 'docId', 'domains', 'end', 'geneIds', 'ref', 'alt', 'start',
                  'variantId', 'transcriptIds', 'xpos', 'xstart', 'xstop', 'contig']
        for field in fields:
            self.assertEqual(obj[field], DERIVED_DATA[rsid][field])

        self.assertEqual(obj['mainTranscript']['transcript_id'], DERIVED_DATA[rsid]['mainTranscript']['transcript_id'])
