import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import bigquery

from v03_pipeline.lib.misc.gcp import get_service_account_credentials
from v03_pipeline.lib.misc.requests import requests_retry_session

TABLE_NAME_VALIDATION_REGEX = r'datarepo-\w+:datarepo_\w+'
TDR_ROOT_URL = 'https://data.terra.bio/api/repository/v1/'


def _tdr_request(resource: str) -> dict:
    service_account_token = get_service_account_credentials().token
    s = requests_retry_session()
    res = s.get(
        url=os.path.join(TDR_ROOT_URL, resource),
        headers={'Authorization': f'Bearer {service_account_token}'},
        timeout=10,
    )
    res.raise_for_status()
    return res.json()


def _bigquery_result(table_name: str):
    if not re.match(TABLE_NAME_VALIDATION_REGEX, table_name):
        msg = f'{table_name} does not match expected pattern'
        raise ValueError(msg)
    client = bigquery.Client()
    return client.query_and_wait(
        f"""
        SELECT sample_id, predicted_sex
        FROM `{table_name}.sample`
    """,
    )


def get_dataset_ids() -> list[str]:
    res_body = _tdr_request('datasets')
    items = res_body['items']
    for item in items:
        if not any(x['cloudResource'] == 'bigquery' for x in item['storage']):
            msg = 'Datasets without bigquery sources are unsupported'
            raise ValueError(msg)
    return [x['id'] for x in items]


def get_bigquery_table_names() -> list[str]:
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(
                _tdr_request,
                f'datasets/{dataset_id}?include=ACCESS_INFORMATION',
            )
            for dataset_id in get_dataset_ids()
        ]
        for future in as_completed(futures):
            result = future.result()
            results.append(f"{result['accessInformation']['bigQuery']['projectId']}.{result['accessInformation']['bigQuery']['datasetName']}")
    return results


def get_bigquery_results(table_names: list[str]):
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(_bigquery_result, table_name) for table_name in table_names
        ]
        return [future.result() for future in as_completed(futures)]
