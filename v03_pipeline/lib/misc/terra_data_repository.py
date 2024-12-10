import os

from v03_pipeline.lib.misc.gcp import get_service_account_credentials
from v03_pipeline.lib.misc.requests import requests_retry_session

TDR_ROOT_URL = 'https://data.terra.bio/api/repository/v1/'


def tdr_request(resource: str) -> dict:
    service_account_token = get_service_account_credentials().token
    s = requests_retry_session()
    res = s.get(
        url=os.path.join(TDR_ROOT_URL, resource),
        headers={'Authorization', f'Bearer {service_account_token}'},
        timeout=10,
    )
    res.raise_for_status()
    return res.json()


def get_bigquery_dataset_ids() -> list[str]:
    res_body = tdr_request('/datasets')
    items = res_body['items']
    for item in items:
        if not any(x['cloudResource'] == 'bigquery' for x in item['storage']):
            msg = 'Datasets without bigquery sources are unsupported'
            raise ValueError(msg)
    return [x['id'] for x in items]
