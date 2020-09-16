import datetime
import elasticsearch
import inspect
import logging
import time
from pprint import pformat


handlers = set(logging.root.handlers)
logging.root.handlers = list(handlers)
logger = logging.getLogger()

LOADING_NODES_NAME = 'elasticsearch-es-data-loading*'


class ElasticsearchClient:

    def __init__(self, host='localhost', port='9200', http_auth=None):
        """Constructor.

        Args:
            host (str): Elasticsearch server host
            port (str): Elasticsearch server port
        """

        self._host = host
        self._port = port

        self.es = elasticsearch.Elasticsearch(host, port=port, http_auth=http_auth)

        # check connection
        logger.info(pformat(self.es.info()))

    def create_index(
        self,
        index_name,
        elasticsearch_schema,
        num_shards=1,
        _meta=None,
    ):
        """Calls es.indices.create to create an elasticsearch index with the appropriate mapping.

        Args:
            index_name (str): elasticsearch index mapping
            elasticsearch_schema (dict): elasticsearch mapping "properties" dictionary
            num_shards (int): how many shards the index will contain
            _meta (dict): optional _meta info for this index
                (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-meta-field.html)
        """

        index_mapping = self._format_index_mapping(elasticsearch_schema, _meta)

        body = {
            'mappings': index_mapping,
            'settings': {
                'number_of_shards': num_shards,
                'number_of_replicas': 0,
                'index.mapping.total_fields.limit': 10000,
                'index.refresh_interval': -1,
                'index.codec': 'best_compression',   # halves disk usage, no difference in query times
            }
        }

        logger.info('create_mapping - elasticsearch schema: \n' + pformat(elasticsearch_schema))

        logger.info('==> creating elasticsearch index {}'.format(index_name))
        self.es.indices.create(index=index_name, body=body)

    # TODO unused?
    def update_mapping(self, index_name, elasticsearch_schema, _meta=None):
        """Calls es.indices.put_mapping to update an elasticsearch index mapping.

        Args:
            index_name (str): elasticsearch index mapping
            elasticsearch_schema (dict): elasticsearch mapping "properties" dictionary
            _meta (dict): optional _meta info for this index
                (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-meta-field.html)
        """

        index_mapping = self._format_index_mapping(elasticsearch_schema, _meta)

        logger.info('==> updating elasticsearch index {}. New schema:\n{}'.format(index_name, pformat(elasticsearch_schema)))

        self.es.indices.put_mapping(index=index_name, body=index_mapping)

    @classmethod
    def _format_index_mapping(cls, elasticsearch_schema, _meta):
        index_mapping = {
            'properties': elasticsearch_schema,
        }

        if _meta:
            logger.info('==> index _meta: ' + pformat(_meta))
            index_mapping['_meta'] = _meta

        return index_mapping

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
