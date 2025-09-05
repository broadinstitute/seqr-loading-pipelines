import csv
import gzip
import io

import hail as hl
import hailtop.fs as hfs

from v03_pipeline.lib.misc.clickhouse import get_clickhouse_client
from v03_pipeline.lib.model.environment import Env
from v03_pipeline.lib.paths import db_id_to_gene_id_path

SEQR_POSTGRES_NAMED_COLLECTION = 'seqr_postgres_named_collection'


def db_id_to_gene_ids_exists():
    return hfs.exists(db_id_to_gene_id_path())


def load_db_id_to_gene_ids():
    return hl.dict(
        [
            (gene_id, int(db_id))
            for line in gzip.decompress(
                hfs.open(db_id_to_gene_id_path(), 'rb').read(),
            ).split()
            for db_id, gene_id in [line.decode().split(',', 1)]
        ],
    )


def write_db_id_to_gene_ids():
    client = get_clickhouse_client()
    res = client.execute(
        f"""
        SELECT db_id, gene_id FROM postgresql({CLICKHOUSE_SEARCH_NAMED_COLLECTION}, database='reference_data_db', table='reference_data_geneinfo')
        """,  # noqa: S608
    )
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    for line in res:
        writer.writerow(line)
    compressed = gzip.compress(buffer.getvalue().encode('utf-8'))
    with hfs.open(db_id_to_gene_id_path(), 'w') as f:
        f.write(compressed)
