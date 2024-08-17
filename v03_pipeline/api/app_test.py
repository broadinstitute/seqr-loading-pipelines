import aiohttp
from aiohttp.test_utils import AioHTTPTestCase

from v03_pipeline.api.app import init_web_app
from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType


class AppTest(AioHTTPTestCase):
    async def get_application(self):
        return await init_web_app()

    async def test_status(self):
        async with self.client.request('GET', '/status') as resp:
            self.assertEqual(resp.status, 200)
            resp_json = await resp.json()
        self.assertDictEqual(resp_json, {'success': True})

    async def test_loading_pipeline_invalid_requests(self):
        with self.assertLogs(level='ERROR') as log:
            async with self.client.request('GET', '/loading_pipeline') as resp:
                self.assertEqual(
                    resp.status,
                    aiohttp.web_exceptions.HTTPMethodNotAllowed.status_code,
                )
                self.assertTrue(
                    'HTTPMethodNotAllowed' in log.output[0]
                )

        with self.assertLogs(level='ERROR') as log:
            async with self.client.request('POST', '/loading_pipeline') as resp:
                self.assertEqual(
                    resp.status,
                    aiohttp.web_exceptions.HTTPUnprocessableEntity.status_code,
                )
                self.assertTrue(
                    'HTTPUnprocessableEntity' in log.output[0]
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
                '/loading_pipeline',
                json=body,
            ) as resp:
                self.assertEqual(
                    resp.status,
                    aiohttp.web_exceptions.HTTPBadRequest.status_code,
                )
                self.assertTrue(
                    'callset_path must point to a file that exists' in log.output[0],
                )
