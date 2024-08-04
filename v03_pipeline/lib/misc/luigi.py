import luigi
from luigi.parameter import OptionalParameterMixin


class OptionalEnumListParameter(OptionalParameterMixin, luigi.EnumListParameter):
    expected_type = tuple  # matches expected_type of OptionalListParameter
