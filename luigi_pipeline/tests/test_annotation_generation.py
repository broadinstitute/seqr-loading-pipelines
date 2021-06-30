import unittest
from luigi_pipeline.lib.model.base_mt_schema import row_annotation, RowAnnotation, BaseMTSchema


class TestAnnotatorSchema(BaseMTSchema):
    """These annotators don't work on real data"""

    @row_annotation()
    def annotator_1(self):
        return self.mt.annotator_1

    @row_annotation(name='second_annotator', fn_require=annotator_1)
    def annotator_2(self):
        return self.mt.annotator_2

    @row_annotation(fn_require=[annotator_1, annotator_2])
    def annotator_3(self):
        return self.mt.annotator_3


class TestAnnotator2Schema(TestAnnotatorSchema):
    # fn_require should be the annotation name (not the function name)
    @row_annotation(fn_require='second_annotator')
    def annotator_4(self):
        return self.mt.annotator_2 + 1


class TestAnnotationGeneration(unittest.TestCase):

    def test_annotation_ordering(self):
        annotator = TestAnnotator2Schema(mt=None)
        annotations = annotator.all_annotation_fns()
        self.assertEqual(4, len(annotations))
        batched_annotations = RowAnnotation.determine_annotation_batch_order(annotations)
        # 3 rounds
        self.assertEqual(3, len(batched_annotations))
        self.assertEqual(1, len(batched_annotations[0]))
        self.assertEqual('annotator_1', batched_annotations[0][0].name)
        self.assertEqual(1, len(batched_annotations[1]))
        self.assertEqual('second_annotator', batched_annotations[1][0].name)
        self.assertEqual(2, len(batched_annotations[2]))
        self.assertEqual('annotator_3', batched_annotations[2][0].name)
        self.assertEqual('annotator_4', batched_annotations[2][1].name)


