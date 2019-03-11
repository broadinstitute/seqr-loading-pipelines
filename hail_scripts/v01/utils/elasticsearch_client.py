import logging

from hail_scripts.shared.elasticsearch_client import ElasticsearchClient as BaseElasticsearchClient
from hail_scripts.shared.elasticsearch_utils import (
    ELASTICSEARCH_CREATE,
    ELASTICSEARCH_INDEX,
    ELASTICSEARCH_UPDATE,
    ELASTICSEARCH_UPSERT,
    _encode_field_name,
)
from hail_scripts.v01.utils.elasticsearch_utils import (
    VARIANT_GENOTYPE_FIELDS_TO_EXPORT,
    VARIANT_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP,
    generate_elasticsearch_schema,
    generate_vds_make_table_arg,
    parse_vds_schema)

handlers = set(logging.root.handlers)
logging.root.handlers = list(handlers)

from pprint import pformat

logger = logging.getLogger()


class ElasticsearchClient(BaseElasticsearchClient):

    def export_vds_to_elasticsearch(
        self,
        vds,
        index_name="data",
        index_type_name="variant",
        genotype_fields_to_export=VARIANT_GENOTYPE_FIELDS_TO_EXPORT,
        genotype_field_to_elasticsearch_type_map=VARIANT_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP,
        export_genotypes_as_nested_field=False,
        export_genotypes_as_child_docs=False,
        discard_missing_genotypes=False,
        block_size=5000,
        num_shards=10,
        elasticsearch_write_operation=ELASTICSEARCH_INDEX,
        ignore_elasticsearch_write_errors=False,
        elasticsearch_mapping_id=None,
        delete_index_before_exporting=True,
        disable_doc_values_for_fields=(),
        disable_index_for_fields=(),
        is_split_vds=True,
        export_globals_to_index_meta=True,
        run_after_index_exists=None,
        verbose=True,
        force_merge=False,
    ):
        """Create a new elasticsearch index to store the records in this keytable, and then export all records to it.

        Args:
            kt (KeyTable): hail KeyTable object.
            genotype_fields_to_export (dict): A dictionary that maps genotype fields (eg 'DP') to hail expressions for
                computing these fields (eg. .
                This will be passed as the 2nd argument to vds.make_table(..)
            genotype_field_to_elasticsearch_type_map (list):
            export_genotypes_as_nested_field (bool): Whether to export genotypes in a "nested" field.
            export_genotypes_as_child_docs (bool): Whether to export genotypes as child docs.
            discard_missing_genotypes (bool): If True, missing (aka. not-called) genotypes won't be exported as nested or child docs.
            index_name (string): elasticsearch index name (equivalent to a database name in SQL)
            index_type_name (string): elasticsearch index type (equivalent to a table name in SQL)
            block_size (int): number of records to write in one bulk insert
            num_shards (int): number of shards to use for this index
                (see https://www.elastic.co/guide/en/elasticsearch/guide/current/overallocation.html)
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
            elasticsearch_mapping_id (string): if specified, sets the es.mapping.id which is the column name to use as the document ID
                See https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html#cfg-mapping
            delete_index_before_exporting (bool): Whether to drop and re-create the index before exporting.
            disable_doc_values_for_fields: (optional) list of field names (the way they will be
                named in the elasticsearch index) for which to not store doc_values
                (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html)
            disable_index_for_fields: (optional) list of field names (the way they will be
                named in the elasticsearch index) that shouldn't be indexed
                (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html)
            is_split_vds (bool): whether split_multi() has been called on this VDS
            export_globals_to_index_meta (bool): whether to add vds.globals object to the index _meta field:
                (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-meta-field.html)
            run_after_index_exists (function): optional function to run after creating the index, but before exporting any data.
            verbose (bool): whether to print schema and stats
            force_merge (bool): whether to force merge the index when the export is done
        """
        child_kt = None
        enable_global_ordinals_for_fields = []

        # compute genotype_fields_list
        if not genotype_fields_to_export:
            genotype_fields_list = []

        elif export_genotypes_as_nested_field:
            # create a new variant-level "genotypes" field that stores an array of genotypes represented as structs
            genotype_struct_expr = ", ".join([
                "{}: {}".format(key, value) for key, value in genotype_fields_to_export.items()
            ] + [
                "sample_id: s",  # add sample_id to each genotype struct to keep track of
            ])

            vds = vds.annotate_variants_expr("va.genotypes = gs.map(g => { %(genotype_struct_expr)s }).collect()" % locals())

            def _add_vds_sample_field(vds, field_name, field_filter):
                enable_global_ordinals_for_fields.append('samples_{}'.format(field_name))
                return vds.annotate_variants_expr(
                    "va.samples_%(field_name)s = va.genotypes.filter(gen => %(field_filter)s).map(gen => gen.sample_id).toSet" % dict(
                        field_name=field_name, field_filter=field_filter
                    ))

            for i in range(1, 3):
                vds = _add_vds_sample_field(vds, field_name='num_alt_{}'.format(i), field_filter='gen.num_alt == {}'.format(i))
            vds = _add_vds_sample_field(vds, field_name='no_call', field_filter='gen.num_alt == -1')

            for i in range(0, 95, 5):
                vds = _add_vds_sample_field(vds, field_name='gq_{}_to_{}'.format(i, i + 5), field_filter='gen.gq >= {0} && gen.gq < {0} + 5'.format(i))

            for i in range(0, 45, 5):
                vds = _add_vds_sample_field(
                    vds, field_name='ab_{}_to_{}'.format(i, i + 5),
                    field_filter='gen.num_alt == 1 && gen.ab * 100 >= {0} && gen.ab * 100 < {0} + 5'.format(i)
                )

            genotype_fields_list = []  # don't add flat genotype columns to the table. The new 'genotypes' field replaces these

        elif export_genotypes_as_child_docs:

            genotype_fields_list = []  # don't add flat genotype columns to the table. Instead a separate index_name+'_genotypes' index will be created for child docs.

            annotate_expr = [
                "%s=%s" % (key, value) for key, value in genotype_fields_to_export.items()
            ] + [
                "sample_id=s",
                "%(elasticsearch_mapping_id)s=va.%(elasticsearch_mapping_id)s" % locals(),
            ]

            child_kt = vds.genotypes_table()
            if discard_missing_genotypes:
                child_kt = child_kt.filter("g.isCalled()", keep=True)

            child_kt = child_kt.annotate(annotate_expr).select(list(genotype_fields_to_export.keys()) + ["sample_id", elasticsearch_mapping_id])

        else:
            genotype_fields_list = [
                "{} = {}".format(key, value) for key, value in genotype_fields_to_export.items()
            ]

        # compute site_fields_list
        field_path_to_field_type_map = parse_vds_schema(vds.variant_schema.fields, current_parent=["va"])
        site_fields_list = sorted(
            generate_vds_make_table_arg(field_path_to_field_type_map, is_split_vds=is_split_vds)
        )

        kt = vds.make_table(
            site_fields_list,
            genotype_fields_list)

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
                logger.info("Renaming column %s to %s" % (column_name, fixed_column_name))
                kt_rename_dict[column_name] = fixed_column_name

        kt = kt.rename(kt_rename_dict)

        _meta = None
        if export_globals_to_index_meta:
            _meta = dict(vds.globals._attrs) if vds.globals else {}

        self.export_kt_to_elasticsearch(
            kt,
            index_name,
            index_type_name,
            block_size=block_size,
            num_shards=num_shards,
            delete_index_before_exporting=delete_index_before_exporting,
            elasticsearch_write_operation=elasticsearch_write_operation,
            ignore_elasticsearch_write_errors=ignore_elasticsearch_write_errors,
            elasticsearch_mapping_id=elasticsearch_mapping_id,
            field_name_to_elasticsearch_type_map=genotype_field_to_elasticsearch_type_map,
            disable_doc_values_for_fields=disable_doc_values_for_fields,
            disable_index_for_fields=disable_index_for_fields,
            enable_global_ordinals_for_fields=enable_global_ordinals_for_fields,
            field_names_replace_dot_with=None,
            run_after_index_exists=run_after_index_exists,
            _meta=_meta,
            child_kt=child_kt,
            parent_doc_name="variant",
            child_doc_name="genotype",
            force_merge=force_merge,
            verbose=verbose)

    def export_kt_to_elasticsearch(
        self,
        kt,
        index_name="data",
        index_type_name="variant",
        block_size=5000,
        num_shards=10,
        delete_index_before_exporting=True,
        elasticsearch_write_operation=ELASTICSEARCH_INDEX,
        ignore_elasticsearch_write_errors=False,
        elasticsearch_mapping_id=None,
        field_name_to_elasticsearch_type_map=None,
        disable_doc_values_for_fields=(),
        disable_index_for_fields=(),
        enable_global_ordinals_for_fields=(),
        field_names_replace_dot_with="_",
        run_after_index_exists=None,
        _meta=None,
        child_kt=None,
        parent_doc_name="variant",
        child_doc_name="genotype",
        verbose=True,
        force_merge=False,
    ):
        """Create a new elasticsearch index to store the records in this keytable, and then export all records to it.

        Args:
            kt (KeyTable): hail KeyTable object.
            index_name (string): elasticsearch index name (equivalent to a database name in SQL)
            index_type_name (string): elasticsearch index type (equivalent to a table name in SQL)
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
            elasticsearch_mapping_id (str): if specified, sets the es.mapping.id which is the field name to use as the document ID
                See https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html#cfg-mapping
            field_name_to_elasticsearch_type_map (dict): (optional) a map of keytable field names to
                their elasticsearch field spec - for example: {
                    'allele_freq': { 'type': 'half_float' },
                    ...
                }.
                See https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html for
                more details. Any values in this dictionary will override
                the default type mapping derived from the hail keytable column type.
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
            run_after_index_exists (function): optional function to run after creating the index, but before exporting any data.
            _meta (dict): optional _meta info for this index
                (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-meta-field.html)
            child_kt (KeyTable): if not None, this KeyTable will be exported after the main KeyTable (which was passed in the kt arg) is exported.
            parent_doc_name (str): if a child_kt is provided, this allows the name of parent documents to be customized (see https://www.elastic.co/guide/en/elasticsearch/reference/current/parent-join.html)
            child_doc_name (str): if a child_kt is provided, this allows the name of child documents to be customized (see https://www.elastic.co/guide/en/elasticsearch/reference/current/parent-join.html)
            verbose (bool): whether to print schema and stats
            force_merge (bool): whether to force merge the index when the export is done
        """

        # output .tsv for debugging
        #kt.export("gs://seqr-hail/temp/%s_%s.tsv" % (index_name, index_type_name))

        elasticsearch_config = {}
        if elasticsearch_write_operation is not None and elasticsearch_write_operation not in [
                ELASTICSEARCH_INDEX, ELASTICSEARCH_CREATE, ELASTICSEARCH_UPDATE, ELASTICSEARCH_UPSERT]:
            raise ValueError("Unexpected value for elasticsearch_write_operation arg: " + str(elasticsearch_write_operation))

        if elasticsearch_write_operation is not None:
            elasticsearch_config["es.write.operation"] = elasticsearch_write_operation

        if elasticsearch_write_operation in (ELASTICSEARCH_UPDATE, ELASTICSEARCH_UPSERT):
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
            logger.info("Encoding column name %s to %s" % (original_name, encoded_name))

        kt = kt.rename(rename_dict)

        if verbose:
            logger.info("export_kt_to_elasticsearch - KeyTable schema: " + pformat(kt.schema))

        # create elasticsearch index with fields that match the ones in the keytable
        field_path_to_field_type_map = parse_vds_schema(kt.schema.fields, current_parent=["va"])

        index_schema = generate_elasticsearch_schema(
            field_path_to_field_type_map,
            field_name_to_elasticsearch_type_map=field_name_to_elasticsearch_type_map,
            enable_global_ordinals_for_fields=enable_global_ordinals_for_fields,
            disable_doc_values_for_fields=disable_doc_values_for_fields,
            disable_index_for_fields=disable_index_for_fields)

        if child_kt is not None:
            # see https://www.elastic.co/guide/en/elasticsearch/reference/current/parent-join.html
            index_schema["join_field"] = {
                "type": "join",
                "relations": {
                    parent_doc_name: child_doc_name,
                }
            }
            index_schema['sample_id'] = {'type': 'keyword'}
            # see https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html#cfg-mapping
            elasticsearch_config["es.mapping.join"] = "join_field"
            kt = kt.annotate("join_field='{}'".format(parent_doc_name))

        # optionally delete the index before creating it
        if delete_index_before_exporting and self.es.indices.exists(index=index_name):
            self.es.indices.delete(index=index_name)

        # create/update elasticsearch mapping
        self.create_or_update_mapping(
            index_name,
            index_type_name,
            index_schema,
            num_shards=num_shards,
            _meta=_meta)

        if run_after_index_exists:
            run_after_index_exists()

        # export the data rows to elasticsearch
        logger.info("==> exporting data to elasticasearch. Write mode: %s, blocksize: %s" % (elasticsearch_write_operation, block_size))
        kt.to_dataframe().show(n=5)
        kt.export_elasticsearch(
            self._host,
            int(self._port),
            index_name,
            index_type_name,
            block_size,
            config=elasticsearch_config)

        if child_kt is not None:
            elasticsearch_config["es.write.operation"] = "index"
            if "es.mapping.id" in elasticsearch_config:
                del elasticsearch_config["es.mapping.id"]

            child_kt = child_kt.annotate("join_field={name: '%(child_doc_name)s', parent: %(elasticsearch_mapping_id)s }" % locals())
            child_kt = child_kt.drop([elasticsearch_mapping_id])  # now that this field has been added to join_field, it can be dropped as a separate field

            child_kt.to_dataframe().show(n=5)
            child_kt.export_elasticsearch(
                self._host,
                int(self._port),
                index_name,
                index_type_name,
                block_size,
                config=elasticsearch_config)

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

        self.es.indices.refresh(index=index_name)
        if force_merge:
            self.es.indices.forcemerge(index=index_name)

        if verbose:
            self.print_elasticsearch_stats()
