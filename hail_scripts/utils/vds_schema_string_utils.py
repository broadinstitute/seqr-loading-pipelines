
def parse_field_names_and_types(schema_string, to_keep=True, strip_quotes=True):
    """Parses the string representation of a VDS schema into a list of 2-tuples: (name, type_string)
    For example - given the string:
        '''
            chrom: String,
            pos: Int,
        ''''

        It will yield the tuples:
            ("chrom", "String"), ("pos", "Int")

    Args:
        schema_string: The string created by printing str(vds.variant_schema) or str(keytable.schema).
            For example:
                '''
                    rsid: String,
                    qual: Double,
                    filters: Set[String],
                    pass: Boolean,
                '''
        to_keep (bool): If True, only schema lines that don't start with '---' will be returned.
            If False, only schema lines that do start with '---' will be returned.
        strip_quotes (bool): If True, the ` character will be stripped from field names, so
            a line like
                `chrom`: String,
            will be parsed as ("chrom", "String")

    Yields:
        2-tuple:  For example: ("rsid", "String"), ("qual", "Double"), ("filters", "Set[String]") ..
    """
    for i, name_and_type in enumerate(schema_string.split(',')):
        name_and_type = name_and_type.strip()  # eg. "AF: Array[Double],"
        if not name_and_type:
            continue

        if (name_and_type.startswith("---") and to_keep) or (not name_and_type.startswith("---") and not to_keep):
            continue

        if len(name_and_type.split(": ")) != 2:
            raise ValueError("Could not parse name and type from line %s in schema: '%s'" % (i, name_and_type))

        field_name, field_type = name_and_type.strip(' -,').split(": ")
        if strip_quotes:
            field_name = field_name.strip("`")

        yield field_name, field_type # eg. ("AF", "Array[Double]")


def convert_vds_schema_string_to_annotate_variants_expr(
        top_level_fields="",
        info_fields="",
        other_source_fields="",
        other_source_root="",
        root="",
        split_multi=True):
    """Takes a string representation of the VDS variant_schema and generates a string expression
    that can be passed to hail's annotate_variants_expr function to clean up the data shape to:

    1. flatten the data so that VCF "top_level" fields and "INFO" fields now appear at the same level
    2. discard unused fields
    3. convert all Array-type values to a single value in the underlying primitive type by
        applying [va.aIndex - 1]. This assumes that split_multi() has already been run to
        split multi-allelic variants.

    Args:
        top_level_fields (str): VDS fields that are direct children of the 'va' struct. For example:
            '''
                rsid: String,
                qual: Double,
                filters: Set[String],
                pass: Boolean,
            '''
        info_fields (str): For example:
            '''
                AC: Array[Int],
                AF: Array[Double],
                AN: Int,
            '''
        root (str): Where to attach the new data shape in the 'va' data struct.

    Returns:
        string:

    """
    fields = []
    if top_level_fields:
        fields += [("va", top_level_fields)]
    if info_fields:
        fields += [("va.info", info_fields)]
    if other_source_root and other_source_fields:
        fields += [(other_source_root, other_source_fields)]

    expr_lines = []
    for source_root, fields_string in fields:
        # in some cases aIndex is @ vds.aIndex instead of va.aIndex
        aIndex_root = "va" if source_root in ("v", "va") else source_root.split(".")[0]

        for field_name, field_type in parse_field_names_and_types(fields_string):
            field_expr = "%(root)s.%(field_name)s = %(source_root)s.%(field_name)s" % locals()

            if split_multi and field_type.startswith("Array"):
                field_expr += "[%(aIndex_root)s.aIndex-1]" % locals()

            expr_lines.append(field_expr)

    return ",\n".join(expr_lines)


def convert_vds_schema_string_to_vds_make_table_arg(
        top_level_fields="",
        info_fields="",
        vep_fields="",
        other_source_fields="",
        other_source_root="",
        output_field_name_prefix="",
        split_multi=True):
    """Takes a string representation of the VDS variant schema and converts it to a list that can be
    passed to the vds.make_table(..) function to create a key table.

    Args:
        top_level_fields (str): VDS fields that are direct children of the 'va' struct. For example:
            '''
                rsid: String,
                qual: Double,
                filters: Set[String],
                pass: Boolean,
            '''
        info_fields (str): For example:
            '''
                AC: Array[Int],
                AF: Array[Double],
                AN: Int,
            '''
        root (str):
    Returns:
        list: A list of strings

        For example:
            [
                "AC = va.clean.AC",
                "AF = va.clean.AF",
                ...
            ]

    """

    result = []

    fields = []
    if top_level_fields:
        fields += [("va", top_level_fields)]
    if info_fields:
        fields += [("va.info", info_fields)]
    if other_source_root and other_source_fields:
        fields += [(other_source_root, other_source_fields)]

    for source_root, fields_string in fields:
        for field_name, field_type in parse_field_names_and_types(fields_string):
            field_expr = "%(output_field_name_prefix)s%(field_name)s = %(source_root)s.%(field_name)s" % locals()
            if split_multi and field_type.startswith("Array"):
                field_expr += "[va.aIndex-1]"
            result.append(field_expr)

    return result

