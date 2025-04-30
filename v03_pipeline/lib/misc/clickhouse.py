import clickhouse_connect
from clickhouse_connect import common

from v03_pipeline.lib.model.environment import Env


def get_clickhouse_client() -> clickhouse_connect.driver.client.Client:
    # per the docs, require a new session id for every query
    common.set_setting('autogenerate_session_id', False)
    return clickhouse_connect.get_client(
        host=Env.CLICKHOUSE_SERVICE_HOSTNAME,
        port=Env.CLICKHOUSE_SERVICE_PORT,
        username=Env.CLICKHOUSE_USER,
        password=Env.CLICKHOUSE_PASSWORD,
    )
