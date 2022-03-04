import hail as hl
import logging

import sys

if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

logger = logging.getLogger()

# Elastic search write operations.
# See https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html#_operation
ELASTICSEARCH_INDEX = "index"
ELASTICSEARCH_CREATE = "create"
ELASTICSEARCH_UPDATE = "update"
ELASTICSEARCH_UPSERT = "upsert"
ELASTICSEARCH_WRITE_OPERATIONS = set([
    ELASTICSEARCH_INDEX, ELASTICSEARCH_CREATE, ELASTICSEARCH_UPDATE, ELASTICSEARCH_UPSERT,
])

# make encoded values as human-readable as possible
ES_FIELD_NAME_ESCAPE_CHAR = '$'
ES_FIELD_NAME_BAD_LEADING_CHARS = set(['_', '-', '+', ES_FIELD_NAME_ESCAPE_CHAR])
ES_FIELD_NAME_SPECIAL_CHAR_MAP = {
    '.': '_$dot$_',
    ',': '_$comma$_',
    '#': '_$hash$_',
    '*': '_$star$_',
    '(': '_$lp$_',
    ')': '_$rp$_',
    '[': '_$lsb$_',
    ']': '_$rsb$_',
    '{': '_$lcb$_',
    '}': '_$rcb$_',
}

HAIL_TYPE_TO_ES_TYPE_MAPPING = {
    hl.tint: "integer",
    hl.tint32: "integer",
    hl.tint64: "long",
    hl.tfloat: "double",
    hl.tfloat32: "float",
    hl.tfloat64: "double",
    hl.tstr: "keyword",
    hl.tbool: "boolean",
}


# https://hail.is/docs/devel/types.html
# https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
def _elasticsearch_mapping_for_type(dtype):
    if isinstance(dtype, hl.tstruct):
        return {"properties": {field: _elasticsearch_mapping_for_type(dtype[field]) for field in dtype.fields}}
    if isinstance(dtype, (hl.tarray, hl.tset)):
        element_mapping = _elasticsearch_mapping_for_type(dtype.element_type)
        if isinstance(dtype.element_type, hl.tstruct):
            element_mapping["type"] = "nested"
        return element_mapping
    if isinstance(dtype, hl.tlocus):
        return {"type": "object", "properties": {"contig": {"type": "keyword"}, "position": {"type": "integer"}}}
    if dtype in HAIL_TYPE_TO_ES_TYPE_MAPPING:
        return {"type": HAIL_TYPE_TO_ES_TYPE_MAPPING[dtype]}

    # tdict, ttuple, tlocus, tinterval, tcall
    raise NotImplementedError


def elasticsearch_schema_for_table(table, disable_doc_values_for_fields=(), disable_index_for_fields=()):
    """
    Converts the type of a table's row values into a dictionary that can be plugged in to
    an elasticsearch mapping definition.

    Args:
        table (hail.Table): the table to generate a schema for
        disable_doc_values_for_fields: (optional) list of field names (the way they will be
            named in the elasticsearch index) for which to not store doc_values
            (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html)
        disable_index_for_fields: (optional) list of field names (the way they will be
            named in the elasticsearch index) that shouldn't be indexed
            (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html)
    Returns:
        A dict that can be plugged in to an elasticsearch mapping as the value for "properties".
        (see https://www.elastic.co/guide/en/elasticsearch/guide/current/root-object.html)
    """
    properties = _elasticsearch_mapping_for_type(table.key_by().row_value.dtype)["properties"]

    if disable_doc_values_for_fields:
        logger.info("==> will disable doc values for %s", ", ".join(disable_doc_values_for_fields))
        for es_field_name in disable_doc_values_for_fields:
            if es_field_name not in properties:
                raise ValueError(
                    "'%s' in disable_doc_values_for_fields arg is not in the elasticsearch schema: %s"
                    % (es_field_name, properties)
                )
            properties[es_field_name]["doc_values"] = False

    if disable_index_for_fields:
        logger.info("==> will disable index fields for %s", ", ".join(disable_index_for_fields))
        for es_field_name in disable_index_for_fields:
            if es_field_name not in properties:
                flattened_fields = [key for key in properties if key.startswith(f'{es_field_name}_')]
                if flattened_fields:
                    for flattened_es_field_name in flattened_fields:
                        properties[flattened_es_field_name]["index"] = False
                else:
                    raise ValueError(
                        "'%s' in disable_index_for_fields arg is not in the elasticsearch schema: %s"
                        % (es_field_name, properties)
                    )
            else:
                properties[es_field_name]["index"] = False

    return properties

def encode_field_name(s):
    """Encodes arbitrary string into an elasticsearch field name

    See:
    https://discuss.elastic.co/t/special-characters-in-field-names/10658/2
    https://discuss.elastic.co/t/illegal-characters-in-elasticsearch-field-names/17196/2
    """
    field_name = StringIO()
    for i, c in enumerate(s):
        if c == ES_FIELD_NAME_ESCAPE_CHAR:
            field_name.write(2*ES_FIELD_NAME_ESCAPE_CHAR)
        elif c in ES_FIELD_NAME_SPECIAL_CHAR_MAP:
            field_name.write(ES_FIELD_NAME_SPECIAL_CHAR_MAP[c])  # encode the char
        else:
            field_name.write(c)  # write out the char as is

    field_name = field_name.getvalue()

    # escape 1st char if necessary
    if any(field_name.startswith(c) for c in ES_FIELD_NAME_BAD_LEADING_CHARS):
        return ES_FIELD_NAME_ESCAPE_CHAR + field_name
    else:
        return field_name
