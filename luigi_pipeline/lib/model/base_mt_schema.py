from inspect import getmembers, ismethod
from functools import wraps
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class RowAnnotationOmit(Exception):
    pass


def row_annotation(name=None, fn_require=None):
    """
    Function decorator for methods in a subclass of BaseMTSchema.
    Allows the function to be treated like an row_annotation with annotation name and value.

        @row_annotation()
        def a(self):
            return 'a_val'

        @row_annotation(name='b', fn_require=a)
        def b_1(self):
            return 'b_val'

    Will generate a mt with rows of {a: 'a_val', 'b': 'b_val'} if the function is called.
    TODO: Consider changing fn_require to be a list of requirements.

    When calling the function with annotation already set in the MT, the default behavior is to
    skip unless an overwrite=True is passed into the call.

    :param name: name in the final MT. If not provided, uses the function name.
    :param fn_require: method name strings in class that are dependencies.
    :return:
    """
    def mt_prop_wrapper(func):
        annotation_name = name or func.__name__

        # fn_require checking, done when declared, not called.
        if fn_require:
            if not callable(fn_require):
                raise ValueError('Schema: dependency %s is not of type function.' % fn_require)
            if not hasattr(fn_require, 'mt_cls_meta'):
                raise ValueError('Schema: dependency %s is not a row annotation method.' % fn_require.__name__)

        @wraps(func)
        def wrapper(self, *args, overwrite=False, **kwargs):
            # Called already.
            instance_metadata = self.mt_instance_meta['row_annotations'][wrapper.__name__]
            if instance_metadata['annotated'] > 0:
                return self

            # MT already has annotation, so only continue if overwrite requested.
            if annotation_name in self.mt.rows()._fields:
                logger.warning('MT using schema class %s already has %s annotation.' % (self.__class__, annotation_name))
                if not overwrite:
                    return self
                logger.info('Overwriting matrix table annotation %s' % annotation_name)

            if fn_require:
                getattr(self, fn_require.__name__)()

            try:
                func_ret = func(self, *args, **kwargs)
            # Do not annotate when RowAnnotationOmit raised.
            except RowAnnotationOmit:
                return self

            annotation = {annotation_name: func_ret}
            self.mt = self.mt.annotate_rows(**annotation)

            instance_metadata['annotated'] += 1
            instance_metadata['result'] = func_ret

            return self

        wrapper.mt_cls_meta = {
            'annotated_name': annotation_name
        }
        return wrapper
    return mt_prop_wrapper


class BaseMTSchema:
    """
    Main superclass that provides a Hail MT schema definition. decorate methods with @row_annotation.
    Allows annotations to express dependencies where dependencies are run before (and at most once).
    NOTE: circular dependencies are not supported and not gracefully handled.

    Usage example:
        class TestSchema(BaseMTSchema):

            def __init__(self):
                super(TestSchema, self).__init__(hl.import_vcf('tests/data/1kg_30variants.vcf.bgz'))

            @row_annotation()
            def a(self):
                return 0

            @row_annotation(fn_require=a)
            def b(self):
                return self.a + 1

            @row_annotation(name='c', fn_require=a)
            def c_1(self):
                return self.a + 2

    `TestSchema(mt).b().c_1().select_annotated_mt()` will annotate with {'a': 0, 'b': 1, 'c': 2}

    """
    def __init__(self, mt):
        self.mt = mt
        self.mt_instance_meta = {
            'row_annotations': defaultdict(lambda: {
                'annotated': 0,
                'result': {},
            })
        }

    def all_annotation_fns(self):
        """
        Get all row_annotation decorated methods using introspection.
        :return: list of all annotation functions
        """
        return getmembers(self, lambda x: ismethod(x) and hasattr(x, 'mt_cls_meta'))

    def annotate_all(self, overwrite=False):
        """
        Iterate over all annotation functions and call them on the instance.
        :return: instance object
        """
        for atn_fn in self.all_annotation_fns():
            getattr(self, atn_fn[0])(overwrite=overwrite)
        return self

    def select_annotated_mt(self):
        """
        Returns a matrix table with an annotated rows where each row annotation is a previously called
        annotation (e.g. with the corresponding method or all in case of `annotate_all`).
        :return: a matrix table
        """
        # Selection field is the annotation name of any function that has been called.
        select_fields = []
        for fn in self.all_annotation_fns():
            cls_metadata = fn[1].mt_cls_meta
            if fn[0] in self.mt_instance_meta['row_annotations']:
                inst_fn_metadata = self.mt_instance_meta['row_annotations'][fn[0]]
            else:
                continue

            # Not called.
            if inst_fn_metadata['annotated'] <= 0:
                continue

            select_fields.append(fn[1].mt_cls_meta['annotated_name'])
        return self.mt.select_rows(*select_fields)
