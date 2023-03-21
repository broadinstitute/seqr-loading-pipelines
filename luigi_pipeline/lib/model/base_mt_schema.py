import logging

from typing import List
from inspect import getmembers
from collections import defaultdict

logger = logging.getLogger(__name__)


class RowAnnotationOmit(Exception):
    pass


class RowAnnotationFailed(Exception):
    pass


class RowAnnotation:
    def __init__(self, fn, name=None, disable_index=False, requirements: List[str]=None):
        self.fn = fn
        self.name = name or fn.__name__
        self.disable_index=disable_index
        self.requirements = requirements

    def __repr__(self):
        requires = None
        if self.requirements:
            requires = f' (requires: {", ".join(self.requirements)})'
        return f"{self.name}{requires}"

    def __call__(self: "RowAnnotation", schema: "BaseMTSchema", overwrite: bool = False):
        """
        Call the annotation and track metadata in the calling instance's
        stats dict.
        NB: No dependency resolution here!
        """
        if self.name in schema.mt.rows()._fields and overwrite is False:
            return schema
        schema.mt_instance_meta["row_annotations"][self.name]["result"] = self.fn(schema)
        schema.mt_instance_meta["row_annotations"][self.name]["annotated"] += 1
        return schema


def row_annotation(name=None, disable_index=False, fn_require=None):
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
            for fn in fn_requirements:
                if not isinstance(fn, RowAnnotation):
                    raise ValueError(
                        f'Schema: dependency {(fn_require.__name__ if hasattr(fn_require, "__name__") else str(fn_require))} is not a row annotation method.' 
                    )
            requirements = [fn.name for fn in fn_requirements]

        return RowAnnotation(func, name=name, disable_index=disable_index, requirements=requirements)

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
        rounds: List[List[RowAnnotation]] = [self.all_annotation_fns()]
        logger.debug(f'Will attempt to apply {len(rounds[0])} row annotations')

        while len(rounds) > 0:
            rnd = rounds.pop(0)
            logger.debug(f'Starting round with {len(rnd)} annotations')
            # add callers that you can't run yet to this list
            next_round = []
            annotations_to_apply = {}
            for annotation in rnd:
                # apply each atn_fn here
                instance_metadata = self.mt_instance_meta['row_annotations'][annotation.name]
                if instance_metadata['annotated'] > 0:
                    # already called
                    continue

                # MT already has annotation, so only continue if overwrite requested.
                if annotation.name in self.mt.rows()._fields or annotation.name in annotations_to_apply:
                    logger.warning(
                        'MT using schema class %s already has "%s" annotation.' % (self.__class__.__name__, annotation.name))
                    if not overwrite:
                        continue
                    logger.info(f'Overwriting matrix table annotation {annotation.name}')

                if annotation.requirements and any(r not in called_annotations for r in annotation.requirements):
                    # this annotation has unfulfilled annotations,
                    # so let's do it in the next round
                    next_round.append(annotation)
                    continue

                try:
                    # evaluate the function
                    annotation(self, overwrite=overwrite)
                    annotations_to_apply[annotation.name] = instance_metadata['result']
                except RowAnnotationOmit:
                    # Do not annotate when RowAnnotationOmit raised.
                    logger.debug(f'Received RowAnnotationOmit for "{annotation.name}"')


            # update the mt
            logger.debug('Applying annotations: ' + ', '.join(annotations_to_apply.keys()))
            self.set_mt(self.mt.annotate_rows(**annotations_to_apply))

            called_annotations = called_annotations.union(set(annotations_to_apply.keys()))

            if len(next_round) > 0:
                if len(next_round) == len(rnd):
                    # something has got stuck and it's requirements can't be fulfilled
                    failed_annotations = ', '.join(an.name for an in next_round)
                    flattened_reqs = [inner for an in next_round for inner in (an.requirements or [])]
                    requirements = ', '.join(set(flattened_reqs))
                    raise RowAnnotationFailed(
                        f"Couldn't apply annotations {failed_annotations}, "
                        f"their dependencies could not be fulfilled: {requirements}"
                    )
                rounds.append(next_round)

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

    def get_disable_index_field(self):
        '''
        Retrieve the field indices that should be disabled
        return: list of strings
        '''
        all_fields: List[RowAnnotation] = self.all_annotation_fns()
        disabled_indices = []
        for field in all_fields:
            if field.disable_index == True:
                disabled_indices += [field.name]
  
        return disabled_indices
