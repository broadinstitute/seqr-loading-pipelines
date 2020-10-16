import argparse

import hail as hl

from hail_scripts.v02.utils.elasticsearch_client import ElasticsearchClient


def export_table_to_elasticsearch(table_url, host, index_name, index_type, port=9200, num_shards=1, block_size=200):
    ds = hl.read_table(table_url)

    es = ElasticsearchClient(host, port)
    es.export_table_to_elasticsearch(
        ds,
        index_name=index_name,
        index_type_name=index_type,
        block_size=block_size,
        num_shards=num_shards,
        delete_index_before_exporting=True,
        export_globals_to_index_meta=True,
        verbose=True,
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("table", help="URL of Hail table")
    p.add_argument("--host", help="Elasticsearch host or IP address", required=True)
    p.add_argument("--port", help="Elasticsearch port", default=9200, type=int)
    p.add_argument("--index-name", help="Elasticsearch index name", required=True)
    p.add_argument("--index-type", help="Elasticsearch index type", default='_doc')
    p.add_argument("--num-shards", help="Number of Elasticsearch shards", default=1, type=int)
    p.add_argument("--block-size", help="Elasticsearch block size to use when exporting", default=200, type=int)
    args = p.parse_args()

    export_table_to_elasticsearch(
        args.table, args.host, args.index_name, args.index_type, args.port, args.num_shards, args.block_size
    )


if __name__ == "__main__":
    main()
