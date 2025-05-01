from clickhouse_driver import Client

from v03_pipeline.lib.model.environment import Env


def get_clickhouse_client() -> Client:
    return Client(
        host=Env.CLICKHOUSE_SERVICE_HOSTNAME,
        port=Env.CLICKHOUSE_SERVICE_PORT,
        user=Env.CLICKHOUSE_USER,
        password=Env.CLICKHOUSE_PASSWORD,
    )
