import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch, Mock

import responses

from v03_pipeline.lib.misc.terra_data_repository import (
    TDR_ROOT_URL,
    get_bigquery_dataset_ids,
)


@patch(
    'v03_pipeline.lib.misc.terra_data_repository.get_service_account_credentials',
    return_value=SimpleNamespace(
        token='abcdefg',  # noqa: S106
    ),
)
class TerraDataRepositoryTest(unittest.TestCase):
    @responses.activate
    def test_get_bigquery_dataset_ids(self, _: Mock) -> None:
        responses.get(
            os.path.join(TDR_ROOT_URL, '/datasets'),
            body={
                'total': 3,
                'filteredTotal': 3,
                'items': [
                    {
                        'id': '2dc51ee0-a037-499d-a915-a7c20a0b216d',
                        'name': 'RP_3053',
                        'description': 'TGG_Ware_DRAGEN_hg38. Dataset automatically created and linked to: RP-3053',
                        'defaultProfileId': '0a164b9a-2b8b-45d2-859e-a4e369b9cb4f',
                        'createdDate': '2023-10-05T13:15:27.649760Z',
                        'storage': [
                            {
                                'region': 'us-central1',
                                'cloudResource': 'bigquery',
                                'cloudPlatform': 'gcp',
                            },
                            {
                                'region': 'us-east4',
                                'cloudResource': 'firestore',
                                'cloudPlatform': 'gcp',
                            },
                            {
                                'region': 'us-central1',
                                'cloudResource': 'bucket',
                                'cloudPlatform': 'gcp',
                            },
                        ],
                        'secureMonitoringEnabled': False,
                        'cloudPlatform': 'gcp',
                        'dataProject': 'datarepo-7242affb',
                        'storageAccount': None,
                        'phsId': None,
                        'selfHosted': False,
                        'predictableFileIds': False,
                        'tags': [],
                        'resourceLocks': {
                            'exclusive': None,
                            'shared': [],
                        },
                    },
                    {
                        'id': 'beef77e8-575b-40e5-9340-a6f10e0bec67',
                        'name': 'RP_3056',
                        'description': 'RGP_HMB_DRAGEN_hg38. Dataset automatically created and linked to: RP-3056',
                        'defaultProfileId': '835dd05d-c603-4b0d-926f-d9143fd24549',
                        'createdDate': '2023-10-05T20:15:27.622481Z',
                        'storage': [
                            {
                                'region': 'us-central1',
                                'cloudResource': 'bigquery',
                                'cloudPlatform': 'gcp',
                            },
                            {
                                'region': 'us-east4',
                                'cloudResource': 'firestore',
                                'cloudPlatform': 'gcp',
                            },
                            {
                                'region': 'us-central1',
                                'cloudResource': 'bucket',
                                'cloudPlatform': 'gcp',
                            },
                        ],
                        'secureMonitoringEnabled': False,
                        'cloudPlatform': 'gcp',
                        'dataProject': 'datarepo-5a72e31b',
                        'storageAccount': None,
                        'phsId': None,
                        'selfHosted': False,
                        'predictableFileIds': False,
                        'tags': [],
                        'resourceLocks': {
                            'exclusive': None,
                            'shared': [],
                        },
                    },
                    {
                        'id': 'c8d74ac4-9a2e-4d3d-a6b5-1f4b433d949f',
                        'name': 'RP_3055',
                        'description': 'RGP_GRU_DRAGEN_hg38. Dataset automatically created and linked to: RP-3055',
                        'defaultProfileId': '835dd05d-c603-4b0d-926f-d9143fd24549',
                        'createdDate': '2023-10-05T20:15:28.255304Z',
                        'storage': [
                            {
                                'region': 'us-central1',
                                'cloudResource': 'bigquery',
                                'cloudPlatform': 'gcp',
                            },
                            {
                                'region': 'us-east4',
                                'cloudResource': 'firestore',
                                'cloudPlatform': 'gcp',
                            },
                            {
                                'region': 'us-central1',
                                'cloudResource': 'bucket',
                                'cloudPlatform': 'gcp',
                            },
                        ],
                        'secureMonitoringEnabled': False,
                        'cloudPlatform': 'gcp',
                        'dataProject': 'datarepo-0f5be351',
                        'storageAccount': None,
                        'phsId': None,
                        'selfHosted': False,
                        'predictableFileIds': False,
                        'tags': [],
                        'resourceLocks': {
                            'exclusive': None,
                            'shared': [
                                'Gw_KTeYRS1aLhRvapMLYLg',
                                'WrP-0w1aROOUbgkI8JS6Ug',
                            ],
                        },
                    },
                ],
            },
        )
        self.assertEqual(
            get_bigquery_dataset_ids(),
            [
                '2dc51ee0-a037-499d-a915-a7c20a0b216d',
                'beef77e8-575b-40e5-9340-a6f10e0bec67',
                'c8d74ac4-9a2e-4d3d-a6b5-1f4b433d949f',
            ],
        )
