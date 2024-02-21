import hail as hl
from unittest import mock

from v03_pipeline.lib.model import CachedReferenceDatasetQuery
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase


class UpdatedCachedReferenceDatasetQueryTest(MockedDatarootTestCase):
    @mock.patch(
        'v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query.Globals',
    )
    @mock.patch(
        'v03_pipeline.lib.reference_data.dataset_table_operations.get_dataset_ht',
    )
    def test_updated_crdq_create_table_query_raw_dataset(
        self, mock_get_dataset_ht, mock_globals_class
    ):
        raw_dataset_ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    'alleles': ['A', 'C'],
                    'info': hl.Struct(ALLELEID=0.25),
                },
            ],
        )
        UpdatedCachedReferenceDatasetQuery()
        CachedReferenceDatasetQuery.GNOMAD_CODING_AND_NONCODING_VARIANTS
