import logging

handlers = set(logging.root.handlers)
logging.root.handlers = list(handlers)

import collections
import logging

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


ELASTICSEARCH_MAX_SIGNED_SHORT_INT_TYPE = "32000"

DEFAULT_GENOTYPE_FIELDS_TO_EXPORT = [
    'num_alt = if(g.isCalled()) g.nNonRefAlleles() else -1',
    'gq = if(g.isCalled()) g.gq else NA:Int',
    'ab = let total=g.ad.sum in if(g.isCalled() && total != 0) (g.ad[1] / total).toFloat else NA:Float',
    'dp = if(g.isCalled()) [g.dp, '+ELASTICSEARCH_MAX_SIGNED_SHORT_INT_TYPE+'].min() else NA:Int',
    #'pl = if(g.isCalled) g.pl.mkString(",") else NA:String',  # store but don't index
]

DEFAULT_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP = {
    ".*_num_alt": {"type": "byte", "doc_values": "false"},
    ".*_gq": {"type": "byte", "doc_values": "false"},
    ".*_dp": {"type": "short", "doc_values": "false"},
    ".*_ab": {"type": "half_float", "doc_values": "false"},
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
        disable_doc_values_for_fields=(),
        disable_index_for_fields=()):
    """Converts a dictionary of field names and types to a dictionary that can be plugged in to
    an elasticsearch mapping definition.

    Args:
        field_path_to_field_type_map (dict): a dictionary whose keys are tuples representing the
            path of a field in the VDS schema - for example: ("va", "info", "AC"), and values are
            hail field types as strings - for example "Array[String]".
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
    properties = {}
    for field_path, field_type in field_path_to_field_type_map.items():
        es_field_name = _field_path_to_elasticsearch_field_name(field_path)
        es_type = _map_vds_type_to_es_type(field_type)
        properties[es_field_name] = {"type": es_type}

    if disable_doc_values_for_fields:
        logger.info("==> will disable doc values for %s" % (", ".join(disable_doc_values_for_fields)))
        for es_field_name in disable_doc_values_for_fields:
            if es_field_name not in properties:
                raise ValueError(
                    "'%s' in disable_doc_values_for_fields arg is not in the elasticsearch schema: %s" % (es_field_name, field_path_to_field_type_map))
            properties[es_field_name]["doc_values"] = False

    if disable_index_for_fields:
        logger.info("==> will disable index fields for %s" % (", ".join(disable_index_for_fields)))
        for es_field_name in disable_index_for_fields:
            if es_field_name not in properties:
                raise ValueError(
                    "'%s' in disable_index_for_fields arg is not in the elasticsearch schema: %s" % (es_field_name, disable_index_for_fields))
            properties[es_field_name]["index"] = False

    return properties


def generate_vds_make_table_arg(field_path_to_field_type_map, is_split_vds=True):
    """Converts a dictionary of field names and types into a list that can be passed as an arg to
    vds.make_table(..) in order to create a hail KeyTable with all fields in the passed-in dict

    Args:
        field_path_to_field_type_map (dict): a dictionary whose keys are tuples representing the
            path of a field in the VDS schema - for example: ("va", "info", "AC"), and values are
            hail field types as strings - for example "Array[String]".
        is_split_vds (bool): whether split_multi() has been called on this VDS
    Returns:
        list: A list of strings like [ "AC = va.info.AC[va.aIndex-1]", ... ]

    """
    expr_list = []
    for field_path, field_type in field_path_to_field_type_map.items():
        # drop the 'v', 'va' root from key-table key names
        key = _field_path_to_elasticsearch_field_name(field_path)
        expr = "%s = %s" % (key, ".".join(field_path))
        if is_split_vds and field_type.startswith("Array"):
            expr += "[va.aIndex-1]"
        expr_list.append(expr)

    return expr_list


def parse_vds_schema(vds_variant_schema_fields, current_parent=()):
    """Takes a VDS variant schema fields list (for example: vds.variant_schema.fields)
    and converts it recursively to a field_path_to_field_type_map.

    Args:
        vds_variant_schema_fields (list): hail vds.variant_schema.fields list

    Return:
        dict: a dictionary whose keys are tuples representing the path of a field in the VDS
            schema - for example: ("va", "info", "AC"), and values are hail field types as strings -
            for example "Array[String]".
    """
    field_path_to_field_type_map = {}
    for field in vds_variant_schema_fields:
        field_name = field.name
        field_type = str(field.typ)
        if field_type.startswith("Array") and ".".join(current_parent) not in ["v", "va", "va.info"]:
            raise ValueError(".".join(current_parent)+".%(field_name)s (%(field_type)s): nested array types not yet implemented." % locals())
        if field_type.startswith("Struct"):
            child_schema = parse_vds_schema(field.typ.fields, current_parent + [field_name])
            field_path_to_field_type_map.update(child_schema)
        else:
            field_path_to_field_type_map[tuple(current_parent + [field_name])] = field_type

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

