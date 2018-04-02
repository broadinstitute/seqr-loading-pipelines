# make sure elasticsearch is installed
import os
os.system("pip install elasticsearch")  # this used to be `import pip; pip.main(['install', 'elasticsearch']);`, but pip.main is deprecated as of pip v10

import logging

from utils.elasticsearch_utils import DEFAULT_GENOTYPE_FIELDS_TO_EXPORT, \
    DEFAULT_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP, ELASTICSEARCH_INDEX, parse_vds_schema, \
    generate_vds_make_table_arg, generate_elasticsearch_schema, _encode_field_name, \
    ELASTICSEARCH_CREATE, ELASTICSEARCH_UPDATE, ELASTICSEARCH_UPSERT

handlers = set(logging.root.handlers)
logging.root.handlers = list(handlers)

import elasticsearch
from pprint import pprint, pformat
import re
import time


logger = logging.getLogger()


class ElasticsearchClient:

    def __init__(self, host="localhost", port="9200"):
        """Constructor.

        Args:
            host (str): Elasticsearch server host
            port (str): Elasticsearch server port
        """

        self._host = host
        self._port = port

        self.es = elasticsearch.Elasticsearch(host, port=port)


    def print_elasticsearch_stats(self):
        """Prints elastic search index stats."""

        logger.info("==> Elasticsearch stats:")

        node_stats = self.es.nodes.stats(level="node")
        node_id = node_stats["nodes"].keys()[0]

        logger.info("\n" + str(self.es.cat.indices(v = True, s = "creation.date", h = "creation.date.string,health,index,pri,docs.count,store.size")))

        logger.info("Indices: %s total docs" % node_stats["nodes"][node_id]["indices"]["docs"]["count"])
        logger.info("Free Memory: %0.1f%% (%d Gb out of %d Gb)" % (
            node_stats["nodes"][node_id]["os"]["mem"]["free_percent"],
            node_stats["nodes"][node_id]["os"]["mem"]["free_in_bytes"]/10**9,
            node_stats["nodes"][node_id]["os"]["mem"]["total_in_bytes"]/10**9,
        ))
        logger.info("Free Disk Space: %0.1f%% (%d Gb out of %d Gb)" % (
            (100*node_stats["nodes"][node_id]["fs"]["total"]["free_in_bytes"]/node_stats["nodes"][node_id]["fs"]["total"]["total_in_bytes"]),
            node_stats["nodes"][node_id]["fs"]["total"]["free_in_bytes"]/10**9,
            node_stats["nodes"][node_id]["fs"]["total"]["total_in_bytes"]/10**9,
        ))

        logger.info("CPU load: %s" % str(node_stats["nodes"][node_id]["os"]["cpu"]["load_average"]))
        logger.info("Swap: %s (bytes used)" % str(node_stats["nodes"][node_id]["os"]["swap"]["used_in_bytes"]))
        logger.info("Disk type: " + ("Regular" if node_stats["nodes"][node_id]["fs"]["total"].get("spins") else "SSD"))

        # other potentially interesting fields:
        """
        logger.info("Current HTTP Connections: %s open" % node_stats["nodes"][node_id]["http"]["current_open"])
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

    def export_vds_to_elasticsearch(
        self,
        vds,
        index_name="data",
        index_type_name="variant",
        genotype_fields_to_export=DEFAULT_GENOTYPE_FIELDS_TO_EXPORT,
        genotype_field_to_elasticsearch_type_map=DEFAULT_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP,
        block_size=5000,
        num_shards=10,
        elasticsearch_write_operation=ELASTICSEARCH_INDEX,
        elasticsearch_mapping_id=None,
        delete_index_before_exporting=True,
        disable_doc_values_for_fields=(),
        disable_index_for_fields=(),
        is_split_vds=True,
        verbose=True,
    ):
        """Create a new elasticsearch index to store the records in this keytable, and then export all records to it.

        Args:
            kt (KeyTable): hail KeyTable object.
            genotype_fields_to_export (list): A list of hail expressions for genotype fields to export.
                This will be passed as the 2nd argument to vds.make_table(..)
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
        #    logger.info(pformat((vds.sample_ids))

        field_path_to_field_type_map = parse_vds_schema(vds.variant_schema.fields, current_parent=["va"])
        site_fields_list = sorted(
            generate_vds_make_table_arg(field_path_to_field_type_map, is_split_vds=is_split_vds)
        )

        if genotype_fields_to_export is None:
            genotype_fields_to_export = []

        kt = vds.make_table(
            site_fields_list,
            genotype_fields_to_export,
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
                logger.info("Renaming column %s to %s" % (column_name, fixed_column_name))
                kt_rename_dict[column_name] = fixed_column_name

        kt = kt.rename(kt_rename_dict)

        self.export_kt_to_elasticsearch(
            kt,
            index_name,
            index_type_name,
            block_size=block_size,
            num_shards=num_shards,
            delete_index_before_exporting=delete_index_before_exporting,
            elasticsearch_write_operation=elasticsearch_write_operation,
            elasticsearch_mapping_id=elasticsearch_mapping_id,
            field_name_to_elasticsearch_type_map=genotype_field_to_elasticsearch_type_map,
            disable_doc_values_for_fields=disable_doc_values_for_fields,
            disable_index_for_fields=disable_index_for_fields,
            field_names_replace_dot_with=None,
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
            elasticsearch_mapping_id=None,
            field_name_to_elasticsearch_type_map=None,
            disable_doc_values_for_fields=(),
            disable_index_for_fields=(),
            field_names_replace_dot_with="_",
            verbose=True,
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
            verbose (bool): whether to print schema and stats
        """

        # output .tsv for debugging
        #kt.export("gs://seqr-hail/temp/%s_%s.tsv" % (index_name, index_type_name))


        elasticsearch_config = {}
        if elasticsearch_write_operation is not None:
            if elasticsearch_write_operation not in [
                ELASTICSEARCH_INDEX, ELASTICSEARCH_CREATE, ELASTICSEARCH_UPDATE, ELASTICSEARCH_UPSERT,
            ]:
                raise ValueError("Unexpected value for elasticsearch_write_operation arg: %s" % (
                    elasticsearch_write_operation,))

            elasticsearch_config = {
                "es.write.operation": elasticsearch_write_operation,
            }

            if elasticsearch_mapping_id is not None:
                elasticsearch_config["es.mapping.id"] = elasticsearch_mapping_id

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
            logger.info(pformat(kt.schema))

        # create elasticsearch index with fields that match the ones in the keytable
        field_path_to_field_type_map = parse_vds_schema(kt.schema.fields, current_parent=["va"])

        elasticsearch_schema = generate_elasticsearch_schema(
            field_path_to_field_type_map,
            disable_doc_values_for_fields=disable_doc_values_for_fields,
            disable_index_for_fields=disable_index_for_fields,
        )

        #logger.info(elasticsearch_schema)

        # override elasticsaerch types
        if field_name_to_elasticsearch_type_map is not None:
            modified_elasticsearch_schema = dict(elasticsearch_schema)  # make a copy
            for field_name_regexp, elasticsearch_field_spec in field_name_to_elasticsearch_type_map.items():
                for key, value in elasticsearch_schema.items():
                    if re.match(field_name_regexp, key):
                        modified_elasticsearch_schema[key] = elasticsearch_field_spec
                        break
                else:
                    logger.warn("No columns matched '%s'" % (field_name_regexp,))

            elasticsearch_schema = modified_elasticsearch_schema

        # define the elasticsearch mapping
        elasticsearch_mapping = {
            "settings" : {
                "number_of_shards": num_shards,
                "number_of_replicas": 0,
                "index.mapping.total_fields.limit": 10000,
                "index.refresh_interval": "30s",
                "index.store.throttle.type": "none",
                "index.codec": "best_compression",
            },
            "mappings": {
                index_type_name: {
                    #"_size": {"enabled": "true" },   <--- needs mapper-size plugin to be installed in elasticsearch
                    "_all": {"enabled": "false"},
                    "properties": elasticsearch_schema,
                },
            }
        }

        #logger.info(pformat(elasticsearch_mapping))

        if delete_index_before_exporting and self.es.indices.exists(index=index_name):
            self.es.indices.delete(index=index_name)

        if not self.es.indices.exists(index=index_name):
            logger.info("==> Creating index %s" % index_name)
            self.es.indices.create(index=index_name, body=elasticsearch_mapping)
        else:
            #existing_mapping = self.es.indices.get_mapping(index=index_name, doc_type=index_type_name)
            #logger.info("==> Updating elasticsearch %s schema. Original schema: %s" % (index_name, pformat(existing_mapping)))
            #existing_properties = existing_mapping[index_name]["mappings"][index_type_name]["properties"]
            #existing_properties.update(elasticsearch_schema)

            logger.info("==> Updating elasticsearch %s schema. New schema: %s" % (index_name, pformat(elasticsearch_schema)))
            self.es.indices.put_mapping(index=index_name, doc_type=index_type_name, body={
                "properties": elasticsearch_schema
            })

            #new_mapping = self.es.indices.get_mapping(index=index_name, doc_type=index_type_name)
            #logger.info("==> New elasticsearch %s schema: %s" % (index_name, pformat(new_mapping)))


        logger.info("==> Exporting data to elasticasearch. Write mode: %s, blocksize: %s" % (elasticsearch_write_operation, block_size))
        kt.export_elasticsearch(self._host, int(self._port), index_name, index_type_name, block_size, config=elasticsearch_config)

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

        self.es.indices.forcemerge(index=index_name)

        if verbose:
            self.print_elasticsearch_stats()

    def create_elasticsearch_snapshot(self, index_name, bucket, base_path, snapshot_repo):
        """Creates an elasticsearch snapshot in the given GCS bucket repository.

        NOTE: Elasticsearch must have the GCP snapshot plugin installed - see:
        https://www.elastic.co/guide/en/elasticsearch/plugins/master/repository-gcs.html

        Args:
            index_name (str): snapshot will be created for this index
            bucket (str): GCS bucket name (eg. "my-datasets")
            base_path (str): sub-directory inside the bucket where snapshots are stored
            snapshot_repo (str): the snapshot repository name

        """

        logger.info("==> Check if snapshot repo already exists: %s" % snapshot_repo)
        try:
            repo_info = self.es.snapshot.get_repository(repository=snapshot_repo)
            logger.info(pformat(repo_info))
        except elasticsearch.exceptions.NotFoundError:
            # register repository
            self.create_elasticsearch_snapshot_repository(self._host, self._port, bucket, base_path, snapshot_repo)

        # check that index exists
        matching_indices = self.es.indices.get(index=index_name).keys()
        if not matching_indices:
            all_indices = self.es.indices.get(index="*").keys()
            raise ValueError("%s index not found. Existing indices are: %s" % (index_name, all_indices))

        # see https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html
        snapshot_name = "snapshot_%s__%s" % (index_name.replace("*", "").lower(), time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime()))

        logger.info("==> Creating snapshot in gs://%s/%s/%s" % (bucket, base_path, index_name))
        other_snapshots_running = True
        while other_snapshots_running:
            try:
                logger.info(pformat(
                    self.es.snapshot.create(
                        repository=snapshot_repo,
                        snapshot=snapshot_name,
                        wait_for_completion=False,  # setting this to True wasn't working as expected
                        body={
                            "indices": index_name
                        })
                ))
            except elasticsearch.exceptions.TransportError as e:
                if "concurrent_snapshot_execution_exception" in str(e):
                    logger.info("Wait for other snapshots to complete: " + pformat(
                        self.es.snapshot.status(repository=snapshot_repo)
                    ))
                    time.sleep(3)
                    continue

            other_snapshots_running = False

        logger.info("==> Getting snapshot status for: " + snapshot_name)
        logger.info(pformat(
            self.es.snapshot.status(repository=snapshot_repo)
        ))

    def restore_elasticsearch_snapshot(self, bucket, base_path, snapshot_repo):
        """Restores an Elasticsearch snapshot from the given GCS bucket repository.

        Args:
            bucket (str): GCS bucket name (eg. "my-datasets")
            base_path (str): sub-directory inside the bucket where snapshots are stored
            snapshot_repo (str): the snapshot repository name
        """

        # see https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository-gcs-repository.html
        logger.info("==> Check if snapshot repo exists: %s" % snapshot_repo)
        try:
            repo_info = self.es.snapshot.get_repository(repository=snapshot_repo)
            logger.info(pformat(repo_info))
        except elasticsearch.exceptions.NotFoundError:
            # register repository
            self.create_elasticsearch_snapshot_repository(bucket, base_path, snapshot_repo)

        # see https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html
        #response = requests.get("http://%s:%s/_snapshot/%s/_all" % (args.host, args.port, snapshot_repo))
        #all_snapshots = json.loads(response.content).get("snapshots", [])

        all_snapshots = self.es.snapshot.get(snapshot_repo, "_all").get("snapshots")
        all_snapshots.sort(key=lambda s: s["start_time_in_millis"])

        latest_snapshot = all_snapshots[-1]

        snapshot_name = latest_snapshot["snapshot"]

        # http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.client.SnapshotClient.restore
        logger.info("==> Restoring snapshot: " + snapshot_name)
        logger.info(pformat(
            self.es.snapshot.restore(
                repository=snapshot_repo,
                snapshot=snapshot_name,
                wait_for_completion=False,  # setting this to True wasn't working as expected
            )
        ))

        logger.info("==> Getting snapshot status for: " + snapshot_name)
        logger.info(pformat(
            self.es.snapshot.status(repository=snapshot_repo)
        ))

    def create_elasticsearch_snapshot_repository(self, bucket, base_path, snapshot_repo):
        """Kick off snapshot creation process."""

        logger.info("==> Creating GCS repository %s" % (snapshot_repo, ))

        logger.info("==> Create GCS repository %s" % (snapshot_repo, ))
        body = {
            "type": "gcs",
            "settings": {
                "bucket": bucket,
                "base_path": base_path,
                "compress": True,
            }
        }

        logger.info(pformat(body))

        logger.info(pformat(
            self.es.snapshot.create_repository(repository=snapshot_repo, body=body)
        ))

    def get_elasticsearch_snapshot_status(self, snapshot_repo):
        """Get status of any snapshots being created"""

        return self.es.snapshot.status(repository=snapshot_repo)
