import logging

handlers = set(logging.root.handlers)
logging.root.handlers = list(handlers)

import collections
import logging
import re
import traceback
from hail_scripts.v01.utils.vds_schema_string_utils import parse_field_names_and_types

logger = logging.getLogger()

# valid types:
#   https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
#   long, integer, short, byte, double, float, half_float, scaled_float
#   text and keyword
VDS_TO_ES_TYPE_MAPPING = {
    "Boolean": "boolean",
    "Int":     "integer",
    "Long":    "long",
    "Double":  "half_float",
    "Float":   "half_float",
    "String":  "keyword",
}

# elasticsearch field types for arrays are the same as for simple types:
for vds_type, es_type in VDS_TO_ES_TYPE_MAPPING.items():
    VDS_TO_ES_TYPE_MAPPING.update({"Array[%s]" % vds_type: es_type})
    VDS_TO_ES_TYPE_MAPPING.update({"Set[%s]" % vds_type: es_type})

# nested field support (see https://www.elastic.co/guide/en/elasticsearch/reference/current/nested.html#_using_literal_nested_literal_fields_for_arrays_of_objects)
VDS_TO_ES_TYPE_MAPPING.update({
  'Array[Struct]': 'nested',
})

ELASTICSEARCH_MAX_SIGNED_SHORT_INT_TYPE = "32000"

VARIANT_GENOTYPE_FIELDS_TO_EXPORT = {
    'num_alt': 'if(g.isCalled()) g.nNonRefAlleles() else -1',
    'gq': 'if(g.isCalled()) g.gq else NA:Int',
    'ab': 'let total=g.ad.sum in if(g.isCalled() && total != 0 && g.ad.length > 1) (g.ad[1] / total).toFloat else NA:Float',
    'dp': 'if(g.isCalled()) [g.dp, '+ELASTICSEARCH_MAX_SIGNED_SHORT_INT_TYPE+'].min() else NA:Int',  # compute min() to avoid integer overflow
    #'pl = if(g.isCalled) g.pl.mkString(",") else NA:String',  # store but don't index
}

VARIANT_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP = {
    "(.*[_.]|^)num_alt$": {"type": "byte", "doc_values": "false"},
    "(.*[_.]|^)gq$": {"type": "byte", "doc_values": "false"},
    "(.*[_.]|^)dp$": {"type": "short", "doc_values": "false"},
    "(.*[_.]|^)ab$": {"type": "half_float", "doc_values": "false"},

}


SV_GENOTYPE_FIELDS_TO_EXPORT = {
    'num_alt': 'if(g.GT.isCalled()) g.GT.nNonRefAlleles() else -1',
    #'genotype_filter': 'g.FT',
    #'gq': 'g.GQ',
    'dp': 'if(g.GT.isCalled()) [g.PR.sum + g.SR.sum, '+ELASTICSEARCH_MAX_SIGNED_SHORT_INT_TYPE+'].min() else NA:Int',  # compute min() to avoid integer overflow
    'ab': 'let total=g.PR.sum + g.SR.sum in if(g.GT.isCalled() && total != 0) ((g.PR[1] + g.SR[1]) / total).toFloat else NA:Float',
    'ab_PR': 'let total=g.PR.sum in if(g.GT.isCalled() && total != 0) (g.PR[1] / total).toFloat else NA:Float',
    'ab_SR': 'let total=g.SR.sum in if(g.GT.isCalled() && total != 0) (g.SR[1] / total).toFloat else NA:Float',
    'dp_PR': 'if(g.GT.isCalled()) [g.PR.sum,'+ELASTICSEARCH_MAX_SIGNED_SHORT_INT_TYPE+'].min() else NA:Int',
    'dp_SR': 'if(g.GT.isCalled()) [g.SR.sum,'+ELASTICSEARCH_MAX_SIGNED_SHORT_INT_TYPE+'].min() else NA:Int',
}

SV_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP = {
    "(.*[_.]|^)num_alt$": {"type": "byte", "doc_values": "false"},
    #"(.*[_.]|^)_genotype_filter$": {"type": "keyword", "doc_values": "false"},
    #"(.*[_.]|^)_gq$": {"type": "short", "doc_values": "false"},
    "(.*[_.]|^)_dp$": {"type": "short", "doc_values": "false"},
    "(.*[_.]|^)_ab$": {"type": "half_float", "doc_values": "false"},
    "(.*[_.]|^)_ab_PR$": {"type": "half_float", "doc_values": "false"},
    "(.*[_.]|^)_ab_SR$": {"type": "half_float", "doc_values": "false"},
    "(.*[_.]|^)_dp_PR$": {"type": "short", "doc_values": "false"},
    "(.*[_.]|^)_dp_SR$": {"type": "short", "doc_values": "false"},
}


def _map_vds_type_to_es_type(type_name):
    """Converts a VDS type (eg. "Array[Double]") to an ES type (eg. "float")"""

    es_type = VDS_TO_ES_TYPE_MAPPING.get(type_name)
    if not es_type:
        raise ValueError("Unexpected VDS type: %s" % str(type_name))

    return es_type


def _field_path_to_elasticsearch_field_name(field_path):
    """Take a field_path tuple - for example: ("va", "info", "AC"), and converts it to an
    elasicsearch field name.
    """

    # drop the 'v', 'va' root from elastic search field names
    return "_".join(field_path[1:] if field_path and field_path[0] in ("v", "va") else field_path)


def generate_elasticsearch_schema(
        field_path_to_field_type_map,
        field_name_to_elasticsearch_type_map=None,
        disable_doc_values_for_fields=(),
        disable_index_for_fields=()):
    """Converts a dictionary of field names and types to a dictionary that can be plugged in to
    an elasticsearch mapping definition.

    Args:
        field_path_to_field_type_map (dict): a dictionary whose keys are tuples representing the
            path of a field in the VDS schema - for example: ("va", "info", "AC"), and values are
            hail field types as strings (for example "Array[String]") or dicts representing nested objects.
            The nested objects are used to represent hail "Array[Struct{..}]" and the dict contains a
            recursively-generated field type map for the Struct in Array[Struct{..}].
        field_name_to_elasticsearch_type_map (dict): allows custom elasticsearch field types to be set for certain fields,
            overriding the default types in VDS_TO_ES_TYPE_MAPPING. This should be a dictionary that maps regular
            expressions for field names to the custom elasticsearch type to set for that field. For example,

                field_name_to_elasticsearch_type_map[".*_num_alt"] = {"type": "byte", "doc_values": "false"}

            would change the "num_alt" field from "integer" to "byte" and disable doc values.
        disable_doc_values_for_fields (set): (optional) set of field names (the way they will be
            named in the elasticsearch index) for which to not store doc_values
            (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html)
        disable_index_for_fields (set): (optional) set of field names (the way they will be
            named in the elasticsearch index) that shouldn't be indexed
            (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html)
    Returns:
        A dict that can be plugged in to an elasticsearch mapping as the value for "properties".
        (see https://www.elastic.co/guide/en/elasticsearch/guide/current/root-object.html)
    """
    properties = {}
    for field_path, field_type in field_path_to_field_type_map.items():
        es_field_name = _field_path_to_elasticsearch_field_name(field_path)
        if isinstance(field_type, dict):
            nested_schema = generate_elasticsearch_schema(
                field_type,
                field_name_to_elasticsearch_type_map=field_name_to_elasticsearch_type_map,
                disable_doc_values_for_fields=disable_doc_values_for_fields,
                disable_index_for_fields=disable_index_for_fields)

            properties[es_field_name] = {
                "type": "nested",
                "properties": nested_schema,
            }
            continue

        if field_name_to_elasticsearch_type_map is not None:
            for field_name_regexp, custom_type in field_name_to_elasticsearch_type_map.items():
                if re.match(field_name_regexp, es_field_name):
                    properties[es_field_name] = custom_type
                    break

            if es_field_name in properties:
                continue

        # use the default type
        es_type = _map_vds_type_to_es_type(field_type)
        properties[es_field_name] = {"type": es_type}

    if disable_doc_values_for_fields:
        logger.info("==> will disable doc values for %s" % (", ".join(disable_doc_values_for_fields)))
        for es_field_name in disable_doc_values_for_fields:
            if es_field_name in properties:
                properties[es_field_name]["doc_values"] = False

    if disable_index_for_fields:
        logger.info("==> will disable index fields for %s" % (", ".join(disable_index_for_fields)))
        for es_field_name in disable_index_for_fields:
            if es_field_name in properties:
                properties[es_field_name]["index"] = False

    return properties


def generate_vds_make_table_arg(field_path_to_field_type_map, is_split_vds=True):
    """Converts a dictionary of field names and types into a list that can be passed as an arg to
    vds.make_table(..) in order to create a hail KeyTable with all fields in the passed-in dict

    Args:
        field_path_to_field_type_map (dict): a dictionary whose keys are tuples representing the
            path of a field in the VDS schema - for example: ("va", "info", "AC"), and values are
            hail field types as strings (for example "Array[String]") or dicts representing nested objects.
            The nested objects are used to represent hail "Array[Struct{..}]" and the dict contains a
            recursively-generated field type map for the Struct in Array[Struct{..}].
        is_split_vds (bool): whether split_multi() has been called on this VDS
    Returns:
        list: A list of strings like [ "AC = va.info.AC[va.aIndex-1]", ... ]

    """
    expr_list = []
    for field_path, field_type in field_path_to_field_type_map.items():
        # drop the 'v', 'va' root from key-table key names
        key = _field_path_to_elasticsearch_field_name(field_path)
        expr = "%s = %s" % (key, ".".join(field_path))
        if is_split_vds and isinstance(field_type, basestring) and field_type.startswith("Array"):
            expr += "[va.aIndex-1]"
        expr_list.append(expr)

    return expr_list


def parse_vds_schema(vds_variant_schema_fields, current_parent=()):
    """Takes a VDS variant schema fields list (for example: vds.variant_schema.fields)
    and converts it recursively to a field_path_to_field_type_map.

    Args:
        vds_variant_schema_fields (list): hail vds.variant_schema.fields list
        current_parent (list): for example: ["va", "info"]
    Return:
        dict: a dictionary whose keys are tuples representing the path of a field in the VDS
            schema - for example: ("va", "info", "AC"), and values are hail field types as strings -
            for example "Array[String]".
    """
    if not current_parent:
        current_parent = []

    field_path_to_field_type_map = {}
    for field in vds_variant_schema_fields:
        field_name = field.name
        field_type = str(field.typ)
        field_path = current_parent + [field_name]
        try:
            if field_type.startswith("Array[Struct{"):
                nested_schema = parse_vds_schema(field.typ.element_type.fields, [])
                field_path_to_field_type_map[tuple(field_path)] = nested_schema
            elif field_type.startswith("Struct"):
                struct_schema = parse_vds_schema(field.typ.fields, field_path)
                field_path_to_field_type_map.update(struct_schema)
            else:
                field_path_to_field_type_map[tuple(field_path)] = field_type
        except Exception as e:
            traceback.print_exc()
            raise ValueError("Error processing field: %s[%s]: %s" % (".".join(field_path), field_type, e))

    return field_path_to_field_type_map


def convert_vds_schema_string_to_es_index_properties(
        top_level_fields="",
        info_fields="",
        disable_doc_values_for_fields=(),
        disable_index_for_fields=(),
):
    """Takes a string representation of the VDS variant schema (as generated by running
    pprint(vds.variant_schema)) and converts it to a dictionary that can be plugged in
    to the "properties" section of an Elasticsearch mapping. For example:

    'mappings': {
        'index_type1': {
            'properties': <return value>
        }
    }

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
    Returns:
        dict: a dictionary that represents the "properties" section of an Elasticsearch mapping.

        For example:
            {
                "AC": {"type": "integer"},
                "AF": {"type": "float"},
                "AN": {"type": "integer"},
            }

    """

    properties = collections.OrderedDict()
    for fields_string in (top_level_fields, info_fields):
        field_path_to_field_type_map = {
            (field_name,): field_type for field_name, field_type in parse_field_names_and_types(fields_string)
        }
        elasticsearch_schema = generate_elasticsearch_schema(
            field_path_to_field_type_map,
            disable_doc_values_for_fields=disable_doc_values_for_fields,
            disable_index_for_fields=disable_index_for_fields,
        )
        properties.update(elasticsearch_schema)

    return properties

