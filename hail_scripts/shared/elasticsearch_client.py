import datetime
import inspect
import logging
import time
from pprint import pformat

try:
    import elasticsearch
except ImportError:
    import os
    os.system("pip install elasticsearch")
    import elasticsearch


handlers = set(logging.root.handlers)
logging.root.handlers = list(handlers)
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

        # check connection
        logger.info(pformat(self.es.info()))

    def print_elasticsearch_stats(self):
        """Prints elastic search index stats."""

        logger.info("==> elasticsearch stats:")

        node_stats = self.es.nodes.stats(level="node")
        node_id = node_stats["nodes"].keys()[0]

        logger.info("\n" + str(self.es.cat.indices(
            v=True,
            s="creation.date",
            h="creation.date.string,health,index,pri,docs.count,store.size")))

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

    def create_or_update_mapping(
        self,
        index_name,
        index_type_name,
        elasticsearch_schema={},
        num_shards=1,
        _meta=None,
    ):
        """Calls es.indices.create or es.indices.put_mapping to create or update an elasticsearch index mapping.

        Args:
            index_name (str): elasticsearch index mapping
            index_type_name (str): elasticsearch type
            elasticsearch_schema (dict): elasticsearch mapping "properties" dictionary
            num_shards (int): how many shards the index will contain
            _meta (dict): optional _meta info for this index
                (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-meta-field.html)
        """

        index_mapping = {
            #"_size": {"enabled": "true" },   <--- needs mapper-size plugin to be installed in elasticsearch
            "_all": {"enabled": "false"},
            "properties": elasticsearch_schema,
        }

        if _meta:
            logger.info("==> index _meta: " + pformat(_meta))
            index_mapping["_meta"] = _meta

        # define the elasticsearch mapping
        elasticsearch_mapping = {
            "mappings": {
                index_type_name:
                    index_mapping,
            }
        }

        logger.info("create_or_update_mapping - elasticsearch schema: \n" + pformat(elasticsearch_schema))

        if not self.es.indices.exists(index=index_name):
            logger.info("==> creating elasticsearch index %s" % index_name)
            elasticsearch_mapping["settings"] = {
                "number_of_shards": num_shards,
                "number_of_replicas": 0,
                "index.mapping.total_fields.limit": 10000,
                "index.refresh_interval": -1,
                "index.codec": "best_compression",   # halves disk usage, no difference in query times
                #"index.store.throttle.type": "none",
            }

            self.es.indices.create(index=index_name, body=elasticsearch_mapping)
        else:
            #existing_mapping = self.es.indices.get_mapping(index=index_name, doc_type=index_type_name)
            #logger.info("==> Updating elasticsearch %s schema. Original schema: %s" % (index_name, pformat(existing_mapping)))
            #existing_properties = existing_mapping[index_name]["mappings"][index_type_name]["properties"]
            #existing_properties.update(elasticsearch_schema)

            logger.info("==> updating elasticsearch index %s/%s. New schema:\n%s" % (index_name, index_type_name, pformat(elasticsearch_schema)))

            self.es.indices.put_mapping(index=index_name, doc_type=index_type_name, body=index_mapping)

            #new_mapping = self.es.indices.get_mapping(index=index_name, doc_type=index_type_name)
            #logger.info("==> New elasticsearch %s schema: %s" % (index_name, pformat(new_mapping)))


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

        logger.info("==> check if snapshot repo already exists: %s" % snapshot_repo)
        try:
            repo_info = self.es.snapshot.get_repository(repository=snapshot_repo)
            logger.info(pformat(repo_info))
        except elasticsearch.exceptions.NotFoundError:
            # register repository
            self.create_elasticsearch_snapshot_repository(bucket, base_path, snapshot_repo)

        # check that index exists
        matching_indices = self.es.indices.get(index=index_name).keys()
        if not matching_indices:
            all_indices = self.es.indices.get(index="*").keys()
            raise ValueError("%s index not found. Existing indices are: %s" % (index_name, all_indices))

        # see https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html
        snapshot_name = "snapshot_%s__%s" % (index_name.replace("*", "").lower(), time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime()))

        logger.info("==> creating snapshot in gs://%s/%s/%s" % (bucket, base_path, index_name))
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

        logger.info("==> getting snapshot status for: " + snapshot_name)
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
        logger.info("==> check if snapshot repo exists: %s" % snapshot_repo)
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
        logger.info("==> restoring snapshot: " + snapshot_name)
        logger.info(pformat(
            self.es.snapshot.restore(
                repository=snapshot_repo,
                snapshot=snapshot_name,
                wait_for_completion=False,  # setting this to True wasn't working as expected
            )
        ))

        logger.info("==> getting snapshot status for: " + snapshot_name)
        logger.info(pformat(
            self.es.snapshot.status(repository=snapshot_repo)
        ))

    def create_elasticsearch_snapshot_repository(self, bucket, base_path, snapshot_repo):
        """Kick off snapshot creation process."""

        logger.info("==> creating GCS repository %s" % (snapshot_repo, ))

        logger.info("==> create GCS repository %s" % (snapshot_repo, ))
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

    def save_index_operation_metadata(
        self,
        source_file,
        index_name,
        genome_version,
        fam_file=None,
        remap_sample_ids=None,
        subset_samples=None,
        skip_vep=False,
        project_id=None,
        dataset_type=None,
        sample_type=None,
        command=None,
        directory=None,
        username=None,
        operation="create_index",
        status=None,
    ):
        """Records metadata about the operation"""

        # use inspection to get all arg names and values
        arg_names, _, _, _ = inspect.getargspec(self.save_index_operation_metadata)
        arg_names = arg_names[1:]  # get all except 'self'

        INDEX_OPERATIONS_LOG = "index_operations_log"
        DOC_TYPE = "log"

        schema = {arg_name: {"type": "keyword"} for arg_name in arg_names}
        schema["timestamp"] = {"type": "keyword"}

        self.create_or_update_mapping(INDEX_OPERATIONS_LOG, DOC_TYPE, elasticsearch_schema=schema)

        values = locals()
        body = {key: values.get(key) for key in arg_names}

        body["timestamp"] = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

        self.es.index(index=INDEX_OPERATIONS_LOG, doc_type=DOC_TYPE, op_type="index", body=body)

        logger.info("Saved index operation metadata:")
        logger.info(pformat(body))

    def get_index_meta(self, index_name, index_type_name="*"):
        _meta = {}
        mappings = self.es.indices.get_mapping(index=index_name, doc_type=index_type_name)
        for index_type_name, mapping in mappings.get(index_name, {}).get('mappings', {}).items():
            _meta.update(mapping.get("_meta", {}))

        return _meta

    def set_index_meta(self, index_name, index_type_name, _meta):
        index_mapping = {
            "_meta": _meta,
        }

        self.es.indices.put_mapping(index=index_name, doc_type=index_type_name, body=index_mapping)

    def route_index_to_temp_es_cluster(self, index_name, to_temp_loading):
        """Apply shard allocation filtering rules for the given index to elasticsearch data nodes with *loading* in
        their name:

        If to_temp_loading is True, route new documents in the given index only to nodes named "*loading*".
        Otherwise, move any shards in this index off of nodes named "*loading*"

        Args:
            to_temp_loading (bool): whether to route shards in the given index to the "*loading*" nodes, or move
            shards off of these nodes.
        """
        if to_temp_loading:
            require_name = "es-data-loading*"
            exclude_name = ""
        else:
            require_name = ""
            exclude_name = "es-data-loading*"

        body = {
            "index.routing.allocation.require._name": require_name,
            "index.routing.allocation.exclude._name": exclude_name
        }

        logger.info("==> Setting {}* settings = {}".format(index_name, body))

        index_arg = "{}*".format(index_name)
        self.es.indices.put_settings(index=index_arg, body=body)
