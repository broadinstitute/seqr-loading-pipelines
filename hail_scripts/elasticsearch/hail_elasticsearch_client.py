import logging
import re
from pprint import pformat

import hail as hl

from hail_scripts.elasticsearch.elasticsearch_client_v7 import ElasticsearchClient
from hail_scripts.elasticsearch.elasticsearch_utils import (
    ELASTICSEARCH_INDEX,
    ELASTICSEARCH_UPDATE,
    ELASTICSEARCH_UPSERT,
    ELASTICSEARCH_WRITE_OPERATIONS,
    encode_field_name,
    elasticsearch_schema_for_table,
)


logger = logging.getLogger()


def struct_to_dict(struct):
    return {k: dict(struct_to_dict(v)) if isinstance(v, hl.utils.Struct) else v for k, v in struct.items()}


class HailElasticsearchClient(ElasticsearchClient):
    def export_table_to_elasticsearch(
        self,
        table: hl.Table,
        index_name :str = "data",
        index_type_name :str = '_doc',
        block_size :int = 5000,
        num_shards :int = 10,
        delete_index_before_exporting :bool = True,
        elasticsearch_write_operation :str = ELASTICSEARCH_INDEX,
        ignore_elasticsearch_write_errors :bool = False,
        elasticsearch_mapping_id=None,
        field_name_to_elasticsearch_type_map=None,
        disable_doc_values_for_fields=(),
        disable_index_for_fields=(),
        field_names_replace_dot_with="_",
        func_to_run_after_index_exists=None,
        export_globals_to_index_meta=True,
        verbose=True,
        write_null_values=False,
        elasticsearch_config=None,
    ):
        """Create a new elasticsearch index to store the records in this table, and then export all records to it.

        Args:
            table (Table): hail Table
            index_name (string): elasticsearch index name
            index_type_name (string): elasticsearch index type
            block_size (int): number of records to write in one bulk insert
            num_shards (int): number of shards to use for this index
                (see https://www.elastic.co/guide/en/elasticsearch/guide/current/overallocation.html)
            delete_index_before_exporting (bool): Whether to drop and re-create the index before exporting.
            elasticsearch_write_operation (string): Can be one of these constants:
                    ELASTICSEARCH_INDEX
                    ELASTICSEARCH_CREATE
                    ELASTICSEARCH_UPDATE
                    ELASTICSEARCH_UPSERT
                See https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html#_operation
            ignore_elasticsearch_write_errors (bool): If True, elasticsearch errors will be logged, but won't cause
                the bulk write call to throw an error. This is useful when, for example,
                elasticsearch_write_operation="update", and the desired behavior is to update all documents that exist,
                but to ignore errors for documents that don't exist.
            elasticsearch_mapping_id (str): if specified, sets the es.mapping.id which is the column name to use as the document ID
                See https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html#cfg-mapping
            field_name_to_elasticsearch_type_map (dict): (optional) a map of table field names to
                their elasticsearch field spec - for example: {
                    'allele_freq': { 'type': 'half_float' },
                    ...
                }.
                See https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html for
                more details. Any values in this dictionary will override
                the default type mapping derived from the hail table's row type.
                Field names can be regular expressions.
            disable_doc_values_for_fields (tuple): (optional) list of field names (the way they will be
                named in the elasticsearch index) for which to not store doc_values
                (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html)
            disable_index_for_fields (tuple): (optional) list of field names (the way they will be
                named in the elasticsearch index) that shouldn't be indexed
                (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html)
            field_names_replace_dot_with (string): since "." chars in field names are interpreted in
                special ways by elasticsearch, set this arg to first go through and replace "." with
                this string in all field names. This replacement is not reversible (or atleast not
                unambiguously in the general case) Set this to None to disable replacement, and fall back
                on an encoding that's uglier, but reversible (eg. "." will be converted to "_$dot$_")
            func_to_run_after_index_exists (function): optional function to run after creating the index, but before exporting any data.
            export_globals_to_index_meta (bool): whether to add table.globals object to the index _meta field:
                (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-meta-field.html)
            verbose (bool): whether to print schema and stats
            write_null_values (bool): whether to write fields that are null to the index
            elasticsearch_config: The initial elasticsearch config from the caller
        """

        elasticsearch_config = elasticsearch_config or {}
        if (
            elasticsearch_write_operation is not None
            and elasticsearch_write_operation not in ELASTICSEARCH_WRITE_OPERATIONS
        ):
            raise ValueError(
                "Unexpected value for elasticsearch_write_operation arg: " + str(elasticsearch_write_operation)
            )

        if elasticsearch_write_operation is not None:
            elasticsearch_config["es.write.operation"] = elasticsearch_write_operation

        if elasticsearch_write_operation in (ELASTICSEARCH_UPDATE, ELASTICSEARCH_UPSERT) or write_null_values:
            # see https://www.elastic.co/guide/en/elasticsearch/hadoop/master/spark.html#spark-sql-write
            # "By default, elasticsearch-hadoop will ignore null values in favor of not writing any field at all.
            # If updating/upserting, then existing field values may need to be overwritten with nulls
            elasticsearch_config["es.spark.dataframe.write.null"] = "true"

        if elasticsearch_mapping_id is not None:
            elasticsearch_config["es.mapping.id"] = elasticsearch_mapping_id

        if ignore_elasticsearch_write_errors:
            # see docs in https://www.elastic.co/guide/en/elasticsearch/hadoop/current/errorhandlers.html
            elasticsearch_config["es.write.rest.error.handlers"] = "log"
            elasticsearch_config["es.write.rest.error.handler.log.logger.name"] = "BulkErrors"

        if self._es_password:
            elasticsearch_config.update({
                'es.net.http.auth.user': self._es_username,
                'es.net.http.auth.pass': self._es_password,
            })

        # encode any special chars in column names
        rename_dict = {}
        for field_name in table.row_value.dtype.fields:
            encoded_name = field_name

            # optionally replace . with _ in a non-reversible way
            if field_names_replace_dot_with is not None:
                encoded_name = encoded_name.replace(".", field_names_replace_dot_with)

            # replace all other special chars with an encoding that's uglier, but reversible
            encoded_name = encode_field_name(encoded_name)

            if encoded_name != field_name:
                rename_dict[field_name] = encoded_name

        for original_name, encoded_name in rename_dict.items():
            logger.info("Encoding column name %s to %s", original_name, encoded_name)

        table = table.rename(rename_dict)

        if verbose:
            logger.info(pformat(table.row_value.dtype))

        # create elasticsearch index with fields that match the ones in the table
        elasticsearch_schema = elasticsearch_schema_for_table(
            table,
            disable_doc_values_for_fields=disable_doc_values_for_fields,
            disable_index_for_fields=disable_index_for_fields,
        )

        # override elasticsearch types
        if field_name_to_elasticsearch_type_map is not None:
            modified_elasticsearch_schema = dict(elasticsearch_schema)  # make a copy
            for field_name_regexp, elasticsearch_field_spec in field_name_to_elasticsearch_type_map.items():
                match_count = 0
                for key in elasticsearch_schema.keys():
                    if re.match(field_name_regexp, key):
                        modified_elasticsearch_schema[key] = elasticsearch_field_spec
                        match_count += 1

                logger.info("%d columns matched '%s'", match_count, field_name_regexp)

            elasticsearch_schema = modified_elasticsearch_schema

        # optionally delete the index before creating it
        if delete_index_before_exporting and self.es.indices.exists(index=index_name):
            self.es.indices.delete(index=index_name)

        _meta = None
        if export_globals_to_index_meta:
            _meta = struct_to_dict(hl.eval(table.globals))

        self.create_or_update_mapping(index_name, elasticsearch_schema, num_shards=num_shards, _meta=_meta)

        if func_to_run_after_index_exists:
            func_to_run_after_index_exists()

        logger.info(
            "==> exporting data to elasticsearch. Write mode: %s, blocksize: %d",
            elasticsearch_write_operation,
            block_size,
        )

        hl.export_elasticsearch(
            table, self._host, int(self._port), index_name, index_type_name, block_size, elasticsearch_config, verbose
        )

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

        self.es.indices.forcemerge(index=index_name, request_timeout=60)
