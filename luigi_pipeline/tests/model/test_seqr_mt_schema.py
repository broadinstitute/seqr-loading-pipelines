import unittest

import hail as hl

from lib.model.seqr_mt_schema import SeqrVariantSchema
from tests.data.sample_vep import VEP_DATA, DERIVED_DATA

class TestSeqrModel(unittest.TestCase):

    def _get_filtered_mt(self, rsid='rs35471880'):
        mt = hl.import_vcf('tests/data/1kg_30variants.vcf.bgz')
        mt = hl.split_multi(mt.filter_rows(mt.rsid == rsid))
        return mt

    def test_variant_derived_fields(self):
        rsid = 'rs35471880'
        mt = self._get_filtered_mt(rsid).annotate_rows(**VEP_DATA[rsid])

        seqr_schema = SeqrVariantSchema(mt)
        seqr_schema.sorted_transcript_consequences().doc_id(length=512).variant_id().contig().pos().start().end().ref().alt() \
            .pos().xstart().xstop().xpos().transcript_consequence_terms().transcript_ids().main_transcript().gene_ids() \
            .coding_gene_ids().domains().ac().af().an().annotate_all()
        mt = seqr_schema.select_annotated_mt()

        obj = mt.rows().collect()[0]

        # Cannot do a nested compare because of nested hail objects, so do one by one.
        fields = ['AC', 'AF', 'AN', 'codingGeneIds', 'docId', 'domains', 'end', 'geneIds', 'ref', 'alt', 'start',
                  'variantId', 'transcriptIds', 'xpos', 'xstart', 'xstop', 'contig']
        for field in fields:
            self.assertEqual(obj[field], DERIVED_DATA[rsid][field])

        self.assertEqual(obj['mainTranscript']['transcript_id'], DERIVED_DATA[rsid]['mainTranscript']['transcript_id'])

    def test_variant_genotypes(self):
        mt = self._get_filtered_mt()
        seqr_schema = SeqrVariantSchema(mt)

        mt = seqr_schema.genotypes().select_annotated_mt()
        genotypes = mt.rows().collect()[0].genotypes
        actual = {gen['sample_id']: dict(gen) for gen in genotypes}

        expected = {'HG00731': {'num_alt': 0, 'gq': 99, 'ab': 0.0, 'dp': 73.0, 'sample_id': 'HG00731'},
                    'HG00732': {'num_alt': 0, 'gq': 99, 'ab': 0.0, 'dp': 70.0, 'sample_id': 'HG00732'},
                    'HG00733': {'num_alt': 0, 'gq': 99, 'ab': 0.0, 'dp': 66.0, 'sample_id': 'HG00733'},
                    'NA19675': {'num_alt': 1, 'gq': 99, 'ab': 0.6000000238418579, 'dp': 29.0,
                                'sample_id': 'NA19675'},
                    'NA19678': {'num_alt': 0, 'gq': 78, 'ab': 0.0, 'dp': 28.0, 'sample_id': 'NA19678'},
                    'NA19679': {'num_alt': 1, 'gq': 99, 'ab': 0.3571428656578064, 'dp': 27.0,
                                'sample_id': 'NA19679'},
                    'NA20870': {'num_alt': 1, 'gq': 99, 'ab': 0.5142857432365417, 'dp': 67.0,
                                'sample_id': 'NA20870'},
                    'NA20872': {'num_alt': 1, 'gq': 99, 'ab': 0.5066666603088379, 'dp': 74.0,
                                'sample_id': 'NA20872'},
                    'NA20874': {'num_alt': 0, 'gq': 99, 'ab': 0.0, 'dp': 69.0, 'sample_id': 'NA20874'},
                    'NA20875': {'num_alt': 0, 'gq': 99, 'ab': 0.0, 'dp': 93.0, 'sample_id': 'NA20875'},
                    'NA20876': {'num_alt': 1, 'gq': 99, 'ab': 0.4383561611175537, 'dp': 70.0,
                                'sample_id': 'NA20876'},
                    'NA20877': {'num_alt': 0, 'gq': 99, 'ab': 0.0, 'dp': 76.0, 'sample_id': 'NA20877'},
                    'NA20878': {'num_alt': 0, 'gq': 99, 'ab': 0.0, 'dp': 73.0, 'sample_id': 'NA20878'},
                    'NA20881': {'num_alt': 0, 'gq': 99, 'ab': 0.0, 'dp': 69.0, 'sample_id': 'NA20881'},
                    'NA20885': {'num_alt': 0, 'gq': 99, 'ab': 0.0, 'dp': 82.0, 'sample_id': 'NA20885'},
                    'NA20888': {'num_alt': 0, 'gq': 99, 'ab': 0.0, 'dp': 74.0, 'sample_id': 'NA20888'}}
        self.assertEqual(actual, expected)

    def test_samples_num_alt(self):
        mt = self._get_filtered_mt()
        seqr_schema = SeqrVariantSchema(mt)

        mt = seqr_schema.samples_no_call().samples_num_alt().select_annotated_mt()
        row = mt.rows().flatten().collect()[0]
        self.assertEqual(row.samples_no_call, set())
        self.assertEqual(row['samples_num_alt.1'], {'NA19679', 'NA19675', 'NA20870', 'NA20876', 'NA20872'})
        self.assertEqual(row['samples_num_alt.2'], set())

    def test_samples_gq(self):
        non_empty = {
            'samples_gq.75_to_80': {'NA19678'}
        }
        start = 0
        end = 95
        step = 5

        mt = self._get_filtered_mt()
        seqr_schema = SeqrVariantSchema(mt)

        mt = seqr_schema.samples_gq(start, end, step).select_annotated_mt()
        row = mt.rows().flatten().collect()[0]

        for name, samples in non_empty.items():
            self.assertEqual(row[name], samples)

        for i in range(start, end, step):
            name = 'samples_gq.%i_to_%i' % (i, i+step)
            if name not in non_empty:
                self.assertEqual(row[name], set())

    def test_samples_ab(self):
        non_empty = {
            'samples_ab.35_to_40': {'NA19679'},
            'samples_ab.40_to_45': {'NA20876'},
        }
        start = 0
        end = 45
        step = 5

        mt = self._get_filtered_mt()
        seqr_schema = SeqrVariantSchema(mt)

        mt = seqr_schema.samples_ab(start, end, step).select_annotated_mt()
        row = mt.rows().flatten().collect()[0]

        for name, samples in non_empty.items():
            self.assertEqual(row[name], samples)

        for i in range(start, end, step):
            name = 'samples_ab.%i_to_%i' % (i, i+step)
            if name not in non_empty:
                self.assertEqual(row[name], set())
