import unittest

import hail as hl

from lib.model.base_mt_schema import BaseMTSchema, row_annotation


class TestBaseModel(unittest.TestCase):

    class TestSchema(BaseMTSchema):

        def __init__(self):
            super(TestBaseModel.TestSchema, self).__init__(hl.import_vcf('tests/data/1kg_30variants.vcf.bgz'))

        @row_annotation()
        def a(self):
            return 0

        @row_annotation(fn_require=a)
        def b(self):
            return self.mt.a + 1

        @row_annotation(name='c', fn_require=a)
        def c_1(self):
            return self.mt.a + 2

    def _count_dicts(self, schema):
        return {
            k: v['annotated']
            for k, v in schema.mt_instance_meta['row_annotations'].items()
        }

    def test_schema_called_once_counts(self):
        test_schema = TestBaseModel.TestSchema()
        test_schema.a()
        fns = test_schema.all_annotation_fns()

        count_dict = self._count_dicts(test_schema)
        self.assertEqual(count_dict, {'a': 1})

    def test_schema_independent_counters(self):
        test_schema = TestBaseModel.TestSchema()
        test_schema.a()

        test_schema2 = TestBaseModel.TestSchema()
        test_schema2.b()

        count_dict = self._count_dicts(test_schema)
        self.assertEqual(count_dict, {'a': 1})

    def test_schema_dependencies(self):
        test_schema = TestBaseModel.TestSchema()
        test_schema.b()

        count_dict = self._count_dicts(test_schema)
        self.assertEqual(count_dict, {'a': 1, 'b': 1})

    def test_schema_called_at_most_once(self):
        test_schema = TestBaseModel.TestSchema()
        test_schema.a().b().c_1()

        count_dict = self._count_dicts(test_schema)
        self.assertEqual(count_dict, {'a': 1, 'b': 1, 'c_1': 1})

    def test_schema_annotate_all(self):
        test_schema = TestBaseModel.TestSchema()
        test_schema.annotate_all()

        count_dict = self._count_dicts(test_schema)
        self.assertEqual(count_dict, {'a': 1, 'b': 1, 'c_1': 1})

    def test_schema_mt_select_annotated_mt(self):
        test_schema = TestBaseModel.TestSchema()
        mt = test_schema.annotate_all().select_annotated_mt()
        first_row = mt.rows().take(1)[0]

        self.assertEqual(first_row.a, 0)
        self.assertEqual(first_row.b, 1)
        self.assertEqual(first_row.c, 2)

    def test_fn_require_type_error(self):
        try:
            class TestSchema(BaseMTSchema):

                @row_annotation(fn_require='hello')
                def a(self):
                    return 0
        except ValueError as e:
            self.assertEqual(str(e), 'Schema: dependency hello is not of type function.')
            return True
        self.fail('Did not raise ValueError.')

    def test_fn_require_class_error(self):
        def dummy():
            pass
        try:
            class TestSchema(BaseMTSchema):

                @row_annotation(fn_require=dummy)
                def a(self):
                    return 0
        except ValueError as e:
            self.assertEqual(str(e), 'Schema: dependency dummy is not a row annotation method.')
            return True
        self.fail('Did not raise ValueError.')

    def test_inheritance(self):
        class TestSchemaChild(TestBaseModel.TestSchema):

            @row_annotation(fn_require=TestBaseModel.TestSchema.a)
            def d(self):
                return self.mt.a + 4

        test_schema = TestSchemaChild()
        mt = test_schema.d().select_annotated_mt()
        first_row = mt.rows().take(1)[0]

        self.assertEqual(first_row.a, 0)
        self.assertEqual(first_row.d, 4)

    def test_overwrite_default_false(self):
        # info field is already in our mt.
        class TestSchema(TestBaseModel.TestSchema):

            @row_annotation()
            def info(self):
                return 0

        # should not overwrite.
        test_schema = TestSchema().info()

        count_dict = self._count_dicts(test_schema)
        self.assertEqual(count_dict, {'info': 0})

    def test_overwrite_true(self):
        # info field is already in our mt.
        class TestSchema(TestBaseModel.TestSchema):

            @row_annotation()
            def info(self):
                return 0

        # should overwrite.
        test_schema = TestSchema().info(overwrite=True)

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
        self.assertEqual(count_dict, {'a': 1, 'b': 1, 'c_1': 1, 'info': 0})

    def test_annotate_all_overwrite_true(self):
        # info field is already in our mt.
        class TestSchema(TestBaseModel.TestSchema):

            @row_annotation()
            def info(self):
                return 0

        # should overwrite.
        test_schema = TestSchema().annotate_all(overwrite=True)

        count_dict = self._count_dicts(test_schema)
        self.assertEqual(count_dict, {'a': 1, 'b': 1, 'c_1': 1, 'info': 1})

