import logging

from typing import List, Set
from inspect import getmembers
from collections import defaultdict

logger = logging.getLogger(__name__)


class RowAnnotationOmit(Exception):
    pass


class RowAnnotationFailed(Exception):
    pass


class RowAnnotation:
    def __init__(self, fn, name=None, requirements: List[str]=None):
        self.fn = fn
        self.name = name or fn.__name__
        self.function_name = fn.__name__
        self.requirements = requirements

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def __repr__(self):
        requires = ''
        if self.requirements:
            requires = f' (requires: {", ".join(self.requirements)})'
        name = self.name
        if self.function_name != self.name:
            name = f'{self.function_name} [annotated as "{self.name}"]'
        return f"{name}{requires}"

    @staticmethod
    def determine_annotation_batch_order(annotations: List['RowAnnotation']) -> List[List['RowAnnotation']]:
        """
        From a list of row annotations, generate a list of batches that can be iteratively applied
        to ensure all the requirements (dependencies) in future batches are satisfied.
        Args:
            annotations: List[RowAnnotation], list of row annotations from BaseMtSchema.all_annotation_fns()

        Returns: List[List[RowAnnotation]]
        """
        rounds: List[List[RowAnnotation]] = []

        applied_annotations: Set[str] = set()
        annotations_to_determine = list(annotations)
        while len(annotations_to_determine) > 0:

            annotations_to_check = annotations_to_determine
            annotations_to_check_next_round = []
            next_round = []

            for annotation in annotations_to_check:
                if annotation.requirements and any(r not in applied_annotations for r in annotation.requirements):
                    # this annotation has unfulfilled annotations, let's do it in the next round
                    annotations_to_check_next_round.append(annotation)
                    continue
                else:
                    # all requirements are satisfied
                    next_round.append(annotation)

            for annotation in next_round:
                applied_annotations.add(annotation.name)

            # if we haven't added any annotations to the next round
            if len(next_round) == 0:
                failed_annotations = ', '.join(an.name for an in annotations_to_check)
                flattened_reqs = [inner for an in annotations_to_check for inner in (an.requirements or [])]
                requirements = ', '.join(set(flattened_reqs))
                raise RowAnnotationFailed(
                    f"Couldn't apply annotations {failed_annotations}, "
                    f"their dependencies could not be fulfilled (potential circular dependency, "
                    f"or mispelled requirement): {requirements}"
                )
            rounds.append(next_round)
            annotations_to_determine = annotations_to_check_next_round

        return rounds


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

    When calling the function with annotation already set in the MT, the default behavior is to
    skip unless an overwrite=True is passed into the call.

    :param name: name in the final MT. If not provided, uses the function name.
    :param fn_require: method names in class that are dependencies.
    :return:
    """
    def mt_prop_wrapper(func):
        requirements = None
        if fn_require is not None:
            fn_requirements = fn_require if isinstance(fn_require, list) else [fn_require]
            requirements = []
            for fn in fn_requirements:
                if isinstance(fn, RowAnnotation):
                    requirements.append(fn.name)
                elif isinstance(fn, str):
                    # just presume the dep is valid
                    requirements.append(fn)
                else:
                    raise ValueError('Schema: dependency %s is not a row annotation method.' % fn_require.__name__)

        return RowAnnotation(func, name=name, requirements=requirements)

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
        self._mt = None
        self.set_mt(mt)
        self.mt_instance_meta = {
            'row_annotations': defaultdict(lambda: {
                'annotated': 0,
                'result': {},
            })
        }

    @property
    def mt(self):
        """
        Don't allow direct sets to self.mt to ensure some references are updated
        """
        return self._mt

    # Don't use @mt.setter as it makes inheritance harder
    def set_mt(self, mt):
        """Set mt here"""
        self._mt = mt

    def all_annotation_fns(self):
        """
        Get all row_annotation decorated methods using introspection.
        :return: list of all annotation functions
        """
        return [a[1] for a in getmembers(self, lambda x: isinstance(x, RowAnnotation))]

    def annotate_all(self, overwrite=False):
        """
        Iterate over all annotation functions and call them on the instance.
        :return: instance object
        """
        called_annotations = set()
        all_annotations = self.all_annotation_fns()
        batches: List[List[RowAnnotation]] = RowAnnotation.determine_annotation_batch_order(all_annotations)
        logger.info(f'Will attempt to apply {len(all_annotations)} row annotations in {len(batches)} batches')

        for batch in batches:
            annotations_to_apply = {}
            for annotation in batch:
                # apply each atn_fn here
                instance_metadata = self.mt_instance_meta['row_annotations'][annotation.name]
                if instance_metadata['annotated'] > 0:
                    # already called
                    continue

                # MT already has the annotation, only continue if overwrite is requested.
                if annotation.name in self.mt.rows()._fields:
                    logger.warning(
                        'MT using schema class %s already has "%s" annotation.' % (self.__class__.__name__, annotation.name))
                    if not overwrite:
                        continue
                    logger.info('Overwriting matrix table annotation %s' % annotation.name)

                try:
                    # evaluate the function
                    func_ret = annotation.fn(self)
                except RowAnnotationOmit:
                    # Do not annotate when RowAnnotationOmit raised.
                    logger.debug(f'Received RowAnnotationOmit for "{annotation.name}"')
                    continue

                annotations_to_apply[annotation.name] = func_ret

                instance_metadata['annotated'] += 1
                instance_metadata['result'] = func_ret

            # update the mt
            logger.info('Applying annotations: ' + ', '.join(annotations_to_apply.keys()))
            self.set_mt(self.mt.annotate_rows(**annotations_to_apply))

            called_annotations = called_annotations.union(set(annotations_to_apply.keys()))

        return self

    def select_annotated_mt(self):
        """
        Returns a matrix table with an annotated rows where each row annotation is a previously called
        annotation (e.g. with the corresponding method or all in case of `annotate_all`).
        :return: a matrix table
        """
        # Selection field is the annotation name of any function that has been called.
        select_fields = []
        row_annotations = self.mt_instance_meta['row_annotations']
        for annotation in self.all_annotation_fns():
            if annotation.name not in row_annotations:
                continue

            inst_fn_metadata = row_annotations[annotation.name]
            # Not called.
            if inst_fn_metadata['annotated'] <= 0:
                continue

            select_fields.append(annotation.name)
        return self.mt.select_rows(*select_fields)
