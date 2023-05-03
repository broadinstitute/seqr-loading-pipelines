import unittest

import hail as hl

from luigi_pipeline.lib.model.base_mt_schema import BaseMTSchema, row_annotation


class TestBaseModel(unittest.TestCase):
    class TestSchema(BaseMTSchema):
        def __init__(self):
            super(TestBaseModel.TestSchema, self).__init__(
                hl.import_vcf('tests/data/1kg_30variants.vcf.bgz'),
            )

        @row_annotation()
        def a(self):
            return 10

        @row_annotation(fn_require=a)
        def b(self):
            return 20

        @row_annotation(name='c', fn_require=a)
        def c_1(self):
            return 30

    def _count_dicts(self, schema):
        return {
            k: v['annotated']
            for k, v in schema.mt_instance_meta['row_annotations'].items()
        }

    def test_schema_called_once_counts(self):
        test_schema = TestBaseModel.TestSchema()
        test_schema.a(test_schema)

        count_dict = self._count_dicts(test_schema)
        self.assertEqual(count_dict, {'a': 1})

    def test_schema_independent_counters(self):
        test_schema = TestBaseModel.TestSchema()
        test_schema.a(test_schema)

        test_schema2 = TestBaseModel.TestSchema()
        test_schema2.b(test_schema2)

        count_dict = self._count_dicts(test_schema)
        self.assertEqual(count_dict, {'a': 1})

    def test_schema_called_at_most_once(self):
        test_schema = TestBaseModel.TestSchema()
        test_schema.a(test_schema).b(test_schema).c_1(test_schema)

        count_dict = self._count_dicts(test_schema)
        self.assertEqual(count_dict, {'a': 1, 'b': 1, 'c': 1})

    def test_schema_annotate_all(self):
        test_schema = TestBaseModel.TestSchema()
        test_schema.annotate_all()

        count_dict = self._count_dicts(test_schema)
        self.assertEqual(count_dict, {'a': 1, 'b': 1, 'c': 1})

    def test_schema_mt_select_annotated_mt(self):
        test_schema = TestBaseModel.TestSchema()
        mt = test_schema.annotate_all().select_annotated_mt()
        first_row = mt.rows().take(1)[0]

        self.assertEqual(first_row.a, 10)
        self.assertEqual(first_row.b, 20)
        self.assertEqual(first_row.c, 30)

    def test_fn_require_type_error(self):
        try:

            class TestSchema(BaseMTSchema):
                @row_annotation(fn_require='hello')
                def a(self):
                    return 0

        except ValueError as e:
            self.assertEqual(
                str(e), 'Schema: dependency hello is not a row annotation method.',
            )
            return True
        self.fail('Did not raise ValueError.')
        return None

    def test_fn_require_class_error(self):
        def dummy():
            pass

        try:

            class TestSchema(BaseMTSchema):
                @row_annotation(fn_require=dummy)
                def a(self):
                    return 0

        except ValueError as e:
            self.assertEqual(
                str(e), 'Schema: dependency dummy is not a row annotation method.',
            )
            return True
        self.fail('Did not raise ValueError.')
        return None

    def test_inheritance(self):
        class TestSchemaChild(TestBaseModel.TestSchema):
            @row_annotation(fn_require=TestBaseModel.TestSchema.a)
            def d(self):
                return self.mt.a + 4

        test_schema = TestSchemaChild()
        mt = test_schema.annotate_all().select_annotated_mt()
        first_row = mt.rows().take(1)[0]

        self.assertEqual(first_row.a, 10)
        self.assertEqual(first_row.d, 14)

    def test_overwrite_default_false(self):
        # info field is already in our mt.
        class TestSchema(TestBaseModel.TestSchema):
            @row_annotation()
            def info(self):
                return 0

        # should not overwrite.
        test_schema = TestSchema()
        test_schema.info(test_schema)

        count_dict = self._count_dicts(test_schema)
        self.assertEqual(count_dict, {})

    def test_overwrite_true(self):
        # info field is already in our mt.
        class TestSchema(TestBaseModel.TestSchema):
            @row_annotation()
            def info(self):
                return 0

        # should overwrite.
        test_schema = TestSchema()
        test_schema.info(test_schema, overwrite=True)

        count_dict = self._count_dicts(test_schema)
        self.assertEqual(count_dict, {'info': 1})

    def test_annotate_all_overwrite_defailt_false(self):
        # info field is already in our mt.
        class TestSchema(TestBaseModel.TestSchema):
            @row_annotation()
            def info(self):
                return 0

        # should overwrite.
        test_schema = TestSchema().annotate_all()

        count_dict = self._count_dicts(test_schema)
        self.assertEqual(count_dict, {'a': 1, 'b': 1, 'c': 1, 'info': 0})

    def test_annotate_all_overwrite_true(self):
        # info field is already in our mt.
        class TestSchema(TestBaseModel.TestSchema):
            @row_annotation()
            def info(self):
                return 0

        # should overwrite.
        test_schema = TestSchema().annotate_all(overwrite=True)

        count_dict = self._count_dicts(test_schema)
        self.assertEqual(count_dict, {'a': 1, 'b': 1, 'c': 1, 'info': 1})
