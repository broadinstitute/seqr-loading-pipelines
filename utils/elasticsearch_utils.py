# make sure elasticsearch is installed
import logging

handlers = set(logging.root.handlers)

import pip
pip.main(['install', 'elasticsearch'])

logging.root.handlers = list(handlers)

import collections
import elasticsearch
import logging
from pprint import pprint
import StringIO
import sys

from utils.vds_schema_string_utils import _parse_fields

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


def _encode_field_name(s):
    """Encodes arbitrary string into an elasticsearch field name

    See:
    https://discuss.elastic.co/t/special-characters-in-field-names/10658/2
    https://discuss.elastic.co/t/illegal-characters-in-elasticsearch-field-names/17196/2
    """
    field_name = StringIO.StringIO()
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


def _decode_field_name(field_name):
    """Converts an elasticsearch field name back to the original unencoded string"""

    if field_name.startswith(ES_FIELD_NAME_ESCAPE_CHAR):
        field_name = field_name[1:]

    i = 0
    original_string = StringIO.StringIO()
    while i < len(field_name):
        current_string = field_name[i:]
        if current_string.startswith(2*ES_FIELD_NAME_ESCAPE_CHAR):
            original_string.write(ES_FIELD_NAME_ESCAPE_CHAR)
            i += 2
        else:
            for original_value, encoded_value in ES_FIELD_NAME_SPECIAL_CHAR_MAP.items():
                if current_string.startswith(encoded_value):
                    original_string.write(original_value)
                    i += len(encoded_value)
                    break
            else:
                original_string.write(field_name[i])
                i += 1

    return original_string.getvalue()


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

    for es_field_name in disable_doc_values_for_fields:
        properties[es_field_name]["doc_values"] = False
    for es_field_name in disable_index_for_fields:
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
            (field_name,): field_type for field_name, field_type in _parse_fields(fields_string)
        }
        elasticsearch_schema = generate_elasticsearch_schema(
            field_path_to_field_type_map,
            disable_doc_values_for_fields=disable_doc_values_for_fields,
            disable_index_for_fields=disable_index_for_fields,
        )
        properties.update(elasticsearch_schema)

    return properties


def export_vds_to_elasticsearch(
        vds,
        export_genotypes=False,
        host="10.48.0.105",   #"elasticsearch" #"localhost" #"k8solo-01"
        port=30001, # "9200"
        index_name="data",
        index_type_name="variant",
        block_size=5000,
        num_shards=10,
        delete_index_before_exporting=True,
        disable_doc_values_for_fields=(),
        disable_index_for_fields=(),
        is_split_vds=True,
        verbose=True,
    ):
    """Create a new elasticsearch index to store the records in this keytable, and then export all records to it.

    Args:
        kt (KeyTable): hail KeyTable object.
        host (string): elasticsearch server url or IP address.
        port (int): elasticsearch server port
        index_name (string): elasticsearch index name (equivalent to a database name in SQL)
        index_type_name (string): elasticsearch index type (equivalent to a table name in SQL)
        block_size (int): number of records to write in one bulk insert
        num_shards (int): number of shards to use for this index
            (see https://www.elastic.co/guide/en/elasticsearch/guide/current/overallocation.html)
        delete_index_before_exporting (bool): Whether to drop and re-create the index before exporting.
        disable_doc_values_for_fields: (optional) list of field names (the way they will be
            named in the elasticsearch index) for which to not store doc_values
            (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html)
        disable_index_for_fields: (optional) list of field names (the way they will be
            named in the elasticsearch index) that shouldn't be indexed
            (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html)
        is_split_vds (bool): whether split_multi() has been called on this VDS
        verbose (bool): whether to print schema and stats
    """

    #if verbose:
    #    pprint(vds.sample_ids)

    field_path_to_field_type_map = parse_vds_schema(vds.variant_schema.fields, current_parent=["va"])
    site_fields_list = sorted(
        generate_vds_make_table_arg(field_path_to_field_type_map, is_split_vds=is_split_vds)
    )
    if export_genotypes:
        genotype_fields_list = [
            'num_alt = if(g.isCalled) g.nNonRefAlleles else -1',
            'gq = if(g.isCalled) g.gq else NA:Int',
            'ab = let total=g.ad.sum in if(g.isCalled && total != 0) (g.ad[1] / total).toFloat else NA:Float',
            'dp = if(g.isCalled) [g.dp, 32000].min() else NA:Int',   # use 32000 as the max depth so that it doesn't overflow elasticsearch's signed-short int type.
            #'sum_ad = g.ad.sum',   # for split VDS, this only takes into account 2 alleles at a multi-allelic site
            #'pl = if(g.isCalled) g.pl.mkString(",") else NA:String',  # store but don't index
        ]
    else:
        genotype_fields_list = []

    kt = vds.make_table(
        site_fields_list,
        genotype_fields_list,
    )

    # replace "." with "_" in genotype column names (but leave sample ids unchanged)
    genotype_column_name_fixes = {
        "%s.%s" % (sample_id, genotype_field): "%s_%s" % (sample_id, genotype_field)
            for sample_id in vds.sample_ids
                for genotype_field in ['num_alt', 'gq', 'ab', 'dp']
    }

    # replace "." with "_" in other column names
    kt_rename_dict = genotype_column_name_fixes
    for column_name in kt.columns:
        if column_name not in kt_rename_dict and "." in column_name:
            fixed_column_name = column_name.replace(".", "_")
            print("Renaming column %s to %s" % (column_name, fixed_column_name))
            kt_rename_dict[column_name] = fixed_column_name

    kt = kt.rename(kt_rename_dict)

    export_kt_to_elasticsearch(
        kt,
        host,
        int(port),
        index_name,
        index_type_name,
        block_size=block_size,
        num_shards=num_shards,
        delete_index_before_exporting=delete_index_before_exporting,
        disable_doc_values_for_fields=disable_doc_values_for_fields,
        disable_index_for_fields=disable_index_for_fields,
        field_names_replace_dot_with=None,
        verbose=verbose)


def export_kt_to_elasticsearch(
        kt,
        host="10.48.0.105",   #"elasticsearch-svc" #"localhost" #"k8solo-01"
        port="9200",
        index_name="data",
        index_type_name="variant",
        block_size=5000,
        num_shards=10,
        delete_index_before_exporting=True,
        disable_doc_values_for_fields=(),
        disable_index_for_fields=(),
        field_names_replace_dot_with="_",
        verbose=True,
    ):
    """Create a new elasticsearch index to store the records in this keytable, and then export all records to it.

    Args:
        kt (KeyTable): hail KeyTable object.
        host (string): elasticsearch server url or IP address.
        port (int): elasticsearch server port
        index_name (string): elasticsearch index name (equivalent to a database name in SQL)
        index_type_name (string): elasticsearch index type (equivalent to a table name in SQL)
        block_size (int): number of records to write in one bulk insert
        num_shards (int): number of shards to use for this index 
            (see https://www.elastic.co/guide/en/elasticsearch/guide/current/overallocation.html)
        delete_index_before_exporting (bool): Whether to drop and re-create the index before exporting.
        disable_doc_values_for_fields: (optional) list of field names (the way they will be
            named in the elasticsearch index) for which to not store doc_values
            (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html)
        disable_index_for_fields: (optional) list of field names (the way they will be
            named in the elasticsearch index) that shouldn't be indexed
            (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html)
        field_names_replace_dot_with (string): since "." chars in field names are interpreted in
            special ways by elasticsearch, set this arg to first go through and replace "." with
            this string in all field names. This replacement is not reversible (or atleast not
            unambiguously in the general case) Set this to None to disable replacement, and fall back
            on an encoding that's uglier, but reversible (eg. "." will be converted to "_$dot$_")
        verbose (bool): whether to print schema and stats
    """

    # output .tsv for debugging
    #kt.export("gs://seqr-hail/temp/%s_%s.tsv" % (index_name, index_type_name))

    # encode any special chars in column names
    rename_dict = {}
    for column_name in kt.columns:
        encoded_name = column_name

        # optionally replace . with _ in a non-reversible way
        if field_names_replace_dot_with is not None:
            encoded_name = encoded_name.replace(".", field_names_replace_dot_with)

        # replace all other special chars with an encoding that's uglier, but reversible
        encoded_name = _encode_field_name(encoded_name)

        if encoded_name != column_name:
            rename_dict[column_name] = encoded_name

    for original_name, encoded_name in rename_dict.items():
        print("Encoding column name %s to %s" % (original_name, encoded_name))

    kt = kt.rename(rename_dict)

    if verbose:
        pprint(kt.schema)

    # create elasticsearch index with fields that match the ones in the keytable
    field_path_to_field_type_map = parse_vds_schema(kt.schema.fields, current_parent=["va"])

    elasticsearch_schema = generate_elasticsearch_schema(
        field_path_to_field_type_map,
        disable_doc_values_for_fields=disable_doc_values_for_fields,
        disable_index_for_fields=disable_index_for_fields,
    )

    # set types and disable doc values for genotype fields
    modified_elasticsearch_schema = {}
    for key, value in elasticsearch_schema.items():
        if key.endswith("_num_alt"):
            modified_elasticsearch_schema[key] = {"type": "byte", "doc_values": "false"}
        elif key.endswith("_gq"):
            modified_elasticsearch_schema[key] = {"type": "byte", "doc_values": "false"}
        elif key.endswith("_dp"):
            modified_elasticsearch_schema[key] = {"type": "short", "doc_values": "false"}
        elif key.endswith("_ab"):
            modified_elasticsearch_schema[key] = {"type": "half_float", "doc_values": "false"}
        else:
            modified_elasticsearch_schema[key] = value

    elasticsearch_schema = modified_elasticsearch_schema

    # define the elasticsearch mapping
    elasticsearch_mapping = {
        "settings" : {
            "number_of_shards": num_shards,
            "number_of_replicas": 0,
            "index.mapping.total_fields.limit": 10000,
            "index.refresh_interval": "30s",
            "index.store.throttle.type": "none",
        },
        "mappings": {
            "variant": {
                "_all": {"enabled": "false"},
                "_size": {"enabled": "true" },
                "properties": elasticsearch_schema,
            },
        }
    }

    #pprint(elasticsearch_mapping)

    logger.info("==> Creating index %s" % index_name)
    es = elasticsearch.Elasticsearch(host, port=port)
    if delete_index_before_exporting and es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

    es.indices.create(index=index_name, body=elasticsearch_mapping)

    logger.info("==> Exporting data to elasticasearch. Blocksize: %s" % block_size)

    # export keytable records to this index
    kt.export_elasticsearch(host, int(port), index_name, index_type_name, block_size, config={}) #, config={ "es.nodes.client.only": "true" })

    """
    Potentially useful config settings for export_elasticsearch(..)
    (https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html)

    es.write.operation // default: index (create, update, upsert)
    es.http.timeout // default 1m
    es.http.retries // default 3
    es.batch.size.bytes  // default 1mb
    es.batch.size.entries  // default 1000
    es.batch.write.refresh // default true  (Whether to invoke an index refresh or not after a bulk update has been completed)
    """

    if verbose:
        print_elasticsearch_stats(es)


def print_elasticsearch_stats(es):
    """Prints elastic search index stats.

    Args:
        es (object): An elasticsearch connection object.
    """
    node_stats = es.nodes.stats(level="node")
    node_id = node_stats["nodes"].keys()[0]

    print("==========================")

    for index in es.indices.get('*'):
        print(index)

    print("Indices: %s total docs" % node_stats["nodes"][node_id]["indices"]["docs"]["count"])
    print("Free Memory: %0.1f%% (%d Gb out of %d Gb)" % (
        node_stats["nodes"][node_id]["os"]["mem"]["free_percent"],
        node_stats["nodes"][node_id]["os"]["mem"]["free_in_bytes"]/10**9,
        node_stats["nodes"][node_id]["os"]["mem"]["total_in_bytes"]/10**9,
    ))
    print("Free Disk Space: %0.1f%% (%d Gb out of %d Gb)" % (
        (100*node_stats["nodes"][node_id]["fs"]["total"]["free_in_bytes"]/node_stats["nodes"][node_id]["fs"]["total"]["total_in_bytes"]),
        node_stats["nodes"][node_id]["fs"]["total"]["free_in_bytes"]/10**9,
        node_stats["nodes"][node_id]["fs"]["total"]["total_in_bytes"]/10**9,
    ))

    print("CPU load: %s" % str(node_stats["nodes"][node_id]["os"]["cpu"]["load_average"]))
    print("Swap: %s (bytes used)" % str(node_stats["nodes"][node_id]["os"]["swap"]["used_in_bytes"]))
    print("Disk type: " + ("Regular" if node_stats["nodes"][node_id]["fs"]["total"].get("spins") else "SSD"))
    print("==========================")

    # other potentially interesting fields:
    """
    print("Current HTTP Connections: %s open" % node_stats["nodes"][node_id]["http"]["current_open"])
    [
        u'thread_pool',
        u'transport_address',
        u'http',
        u'name',
        u'roles',
        u'script',
        u'process',
        u'timestamp',
        u'ingest',
        u'breakers',
        u'host',
        u'fs',
        u'jvm',
        u'ip',
        u'indices',
        u'os',
        u'transport',
        u'discovery',
    ]
    """
