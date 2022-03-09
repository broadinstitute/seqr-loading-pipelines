import datetime
import inspect
import logging
import time
from pprint import pformat


try:
    import elasticsearch
except ImportError:
    import os
    os.system("pip install elasticsearch==7.9.1")
    import elasticsearch


handlers = set(logging.root.handlers)
logging.root.handlers = list(handlers)
logger = logging.getLogger()

LOADING_NODES_NAME = 'elasticsearch-es-data-loading*'


class ElasticsearchClient:

    def __init__(self, host='localhost', port='9200', es_username='pipeline', es_password=None):
        """Constructor.

        Args:
            host (str): Elasticsearch server host
            port (str): Elasticsearch server port
            es_username (str): Elasticsearch username
            es_password (str): Elasticsearch password
        """

        self._host = host
        self._port = port
        self._es_username = es_username
        self._es_password = es_password

        http_auth =  (self._es_username, self._es_password) if self._es_password else None

        self.es = elasticsearch.Elasticsearch(host, port=port, http_auth=http_auth)

        # check connection
        logger.info(pformat(self.es.info()))

    def create_index(self, index_name, elasticsearch_schema, num_shards=1, _meta=None):
        """Calls es.indices.create to create an elasticsearch index with the appropriate mapping.

        Args:
            index_name (str): elasticsearch index mapping
            elasticsearch_schema (dict): elasticsearch mapping "properties" dictionary
            num_shards (int): how many shards the index will contain
            _meta (dict): optional _meta info for this index
                (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-meta-field.html)
        """

        self.create_or_update_mapping(
            index_name, elasticsearch_schema, num_shards=num_shards, _meta=_meta, create_only=True,
        )

    def create_or_update_mapping(self, index_name, elasticsearch_schema, num_shards=1, _meta=None, create_only=False):
        """Calls es.indices.create or es.indices.put_mapping to create or update an elasticsearch index mapping.

        Args:
            index_name (str): elasticsearch index mapping
            elasticsearch_schema (dict): elasticsearch mapping "properties" dictionary
            num_shards (int): how many shards the index will contain
            _meta (dict): optional _meta info for this index
                (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-meta-field.html)
            create_only (bool): only allow index creation, throws an error if index already exists
        """

        index_mapping = {
            'properties': elasticsearch_schema,
        }

        if _meta:
            logger.info('==> index _meta: ' + pformat(_meta))
            index_mapping['_meta'] = _meta

        if not self.es.indices.exists(index=index_name):
            body = {
                'mappings': index_mapping,
                'settings': {
                    'number_of_shards': num_shards,
                    'number_of_replicas': 0,
                    'index.mapping.total_fields.limit': 10000,
                    'index.refresh_interval': -1,
                    'index.codec': 'best_compression',  # halves disk usage, no difference in query times
                }
            }

            logger.info('create_mapping - elasticsearch schema: \n' + pformat(elasticsearch_schema))
            logger.info('==> creating elasticsearch index {}'.format(index_name))

            self.es.indices.create(index=index_name, body=body)
        else:
            if create_only:
                raise ValueError('Index {} already exists'.format(index_name))

            logger.info('==> updating elasticsearch index {}. New schema:\n{}'.format(
                index_name, pformat(elasticsearch_schema)))

            self.es.indices.put_mapping(index=index_name, body=index_mapping)

    def route_index_to_temp_es_cluster(self, index_name):
        """
        Apply shard allocation filtering rules to route the given index to elasticsearch loading nodes
        """
        self._update_settings(index_name, {
            'index.routing.allocation.require._name': LOADING_NODES_NAME,
            'index.routing.allocation.exclude._name': ''
        })

    def route_index_off_temp_es_cluster(self, index_name):
        """
        Move any shards in the given index off of loading nodes
        """
        self._update_settings(index_name, {
            'index.routing.allocation.require._name': '',
            'index.routing.allocation.exclude._name': LOADING_NODES_NAME
        })

    def _update_settings(self, index_name, body):
        logger.info('==> Setting {} settings = {}'.format(index_name, body))

        self.es.indices.put_settings(index=index_name, body=body)

    def get_index_meta(self, index_name):
        mappings = self.es.indices.get_mapping(index=index_name)
        return mappings.get(index_name, {}).get('mappings', {}).get('_meta', {})

    def wait_for_shard_transfer(self, index_name, num_attempts=1000):
        """
        Wait for shards to move off of the loading nodes before connecting to seqr 
        """
        for i in range(num_attempts):
            shards = self.es.cat.shards(index=index_name)
            if LOADING_NODES_NAME not in shards:
                logger.warning("Shards are on {}".format(shards))
                return
            logger.warning("Waiting for {} shards to transfer off the es-data-loading nodes: \n{}".format(
                len(shards.strip().split("\n")), shards))
            time.sleep(5)

        raise Exception('Shards did not transfer off loading nodes')
