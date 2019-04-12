from abc import abstractmethod
from inspect import getmembers, isfunction
from functools import wraps


def mt_annotation(annotation=None, fn_require=None):
    """
    Function decorator for methods in a subclass of BaseMTSchema.
    Allows the function to be treated like an mt_annotation with annotation name and value.

        @mt_annotation()
        def a(self):
            return 'a_val'

        @mt_annotation(annotation='b', fn_require='a')
        def b_1(self):
            return 'b_val'

    Will generate a mt with rows of {a: 'a_val', 'b': 'b_val'} if the function is called.

    :param annotation: name in the final MT. If not provided, uses the function name.
    :param fn_require: method name strings in class that are dependencies.
    :return:
    """
    def mt_prop_wrapper(func):
        annotation_name = annotation or func.__name__

        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # Called already.
            if wrapper.mt_prop_meta['annotated'] > 0:
                return self
            if fn_require:
                getattr(self, fn_require)()

            # Annotate and retiurn instance for chaining.
            self.mt = self.mt.annotate_rows(**{annotation_name: func(self, *args, **kwargs)})
            wrapper.mt_prop_meta['annotated'] += 1
            return self

        wrapper.mt_prop_meta = {
            'annotated': 0,  # Counter for number of times called. Should only be 0 or 1.
            'annotated_name': annotation_name
        }
        return wrapper
    return mt_prop_wrapper


class BaseMTSchema:
    """
    Main superclass that provides a Hail MT schema definition. decorate methods with @mt_annotation.
    Allows annotations to express dependencies where dependencies are run before (and at most once).
    NOTE: circular dependencies are not supported and not gracefully handled.

    Usage example:
        class TestSchema(BaseMTSchema):

            def __init__(self):
                super(TestSchema, self).__init__(hl.import_vcf('tests/data/1kg_30variants.vcf.bgz'))

            @mt_annotation()
            def a(self):
                return 0

            @mt_annotation(fn_require='a')
            def b(self):
                return 1

            @mt_annotation(annotation='c', fn_require='a')
            def c_1(self):
                return 2

    `TestSchema(mt).b().c_1().select_annotated_mt()` will annotate with {'a': 0, 'b': 1, 'c': 2}

    """
    def __init__(self, mt):
        self.mt = mt

    def all_annotation_fns(self):
        """
        Get all mt_annotation decorated methods using introspection.
        :return: list of all annotation functions
        """
        return getmembers(self.__class__, lambda x: isfunction(x) and hasattr(x, 'mt_prop_meta'))

    def annotate_all(self):
        """
        Iterate over all annotation functions and call them on the instance.
        :return: instance object
        """
        for atn_fn in self.all_annotation_fns():
            getattr(self, atn_fn[0])()
        return self

    def select_annotated_mt(self):
        """
        Returns a matrix table with an annotated rows where each row annotation is a previously called
        annotation (e.g. with the corresponding method or all in case of `annotate_all`).
        :return: a matrix table
        """
        # Selection field is the annotation name of any function that has been called.
        select_fields = [fn[1].mt_prop_meta['annotated_name'] for fn in self.all_annotation_fns() if
                         'annotated_name' in fn[1].mt_prop_meta and fn[1].mt_prop_meta['annotated'] > 0]
        return self.mt.select_rows(*select_fields)
