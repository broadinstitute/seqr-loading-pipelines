from aiohttp import web_exceptions
from aiohttp.test_utils import AioHTTPTestCase

from v03_pipeline.api.app import init_web_app
from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

CALLSET_PATH = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'


class AppTest(AioHTTPTestCase, MockedDatarootTestCase):
    async def get_application(self):
        return await init_web_app()

    async def test_status(self):
        async with self.client.request('GET', '/status') as resp:
            self.assertEqual(resp.status, 200)
            resp_json = await resp.json()
        self.assertDictEqual(resp_json, {'success': True})

    async def test_missing_route(self):
        with self.assertLogs(level='ERROR') as _:
            async with self.client.request('GET', '/loading_pip') as resp:
                self.assertEqual(resp.status, web_exceptions.HTTPNotFound.status_code)

    async def test_loading_pipeline_invalid_requests(self):
        with self.assertLogs(level='ERROR') as log:
            async with self.client.request('GET', '/loading_pipeline_enqueue') as resp:
                self.assertEqual(
                    resp.status,
                    web_exceptions.HTTPMethodNotAllowed.status_code,
                )
                self.assertTrue(
                    'HTTPMethodNotAllowed' in log.output[0],
                )

        with self.assertLogs(level='ERROR') as log:
            async with self.client.request('POST', '/loading_pipeline_enqueue') as resp:
                self.assertEqual(
                    resp.status,
                    web_exceptions.HTTPUnprocessableEntity.status_code,
                )
                self.assertTrue(
                    'HTTPUnprocessableEntity' in log.output[0],
                )

        body = {
            'callset_path': 'missing.vcf',
            'projects_to_run': ['project_a'],
            'sample_type': SampleType.WGS.value,
            'reference_genome': ReferenceGenome.GRCh38.value,
            'dataset_type': DatasetType.SNV_INDEL.value,
        }
        with self.assertLogs(level='ERROR') as log:
            async with self.client.request(
                'POST',
                '/loading_pipeline_enqueue',
                json=body,
            ) as resp:
                self.assertEqual(
                    resp.status,
                    web_exceptions.HTTPBadRequest.status_code,
                )
                self.assertTrue(
                    'callset_path must point to a file that exists' in log.output[0],
                )

    async def test_loading_pipeline_enqueue(self):
        body = {
            'callset_path': CALLSET_PATH,
            'projects_to_run': ['project_a'],
            'sample_type': SampleType.WGS.value,
            'reference_genome': ReferenceGenome.GRCh38.value,
            'dataset_type': DatasetType.SNV_INDEL.value,
        }
        async with self.client.request(
            'POST',
            '/loading_pipeline_enqueue',
            json=body,
        ) as resp:
            self.assertEqual(
                resp.status,
                web_exceptions.HTTPAccepted.status_code,
            )
            resp_json = await resp.json()
        self.assertDictEqual(
            resp_json,
            {
                'Successfully queued': {
                    'callset_path': 'v03_pipeline/var/test/callsets/1kg_30variants.vcf',
                    'dataset_type': 'SNV_INDEL',
                    'force': False,
                    'ignore_missing_samples_when_remapping': False,
                    'projects_to_run': ['project_a'],
                    'reference_genome': 'GRCh38',
                    'sample_type': 'WGS',
                    'skip_validation': False,
                },
            },
        )
