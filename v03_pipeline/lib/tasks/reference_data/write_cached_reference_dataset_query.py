import luigi

from v03_pipeline.lib.model import (
    CachedReferenceDatasetQuery,
    DatasetType,
    ReferenceGenome,
    SampleType,
)
from v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query import (
    UpdatedCachedReferenceDatasetQuery,
)


class WriteCachedReferenceDatasetQuery(luigi.Task):
    reference_genome = luigi.EnumParameter(enum=ReferenceGenome)
    dataset_type = luigi.EnumParameter(enum=DatasetType)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dynamic_crdq_tasks = set()

    def complete(self) -> bool:
        return len(self.dynamic_crdq_tasks) > 1 and all(
            crdq_task.complete() for crdq_task in self.dynamic_crdq_tasks
        )

    def run(self):
        # https://luigi.readthedocs.io/en/stable/tasks.html#dynamic-dependencies
        for crdq in CachedReferenceDatasetQuery.for_reference_genome_dataset_type(
            self.reference_genome,
            self.dataset_type,
        ):
            self.dynamic_crdq_tasks.add(
                UpdatedCachedReferenceDatasetQuery(
                    **self.param_kwargs,
                    sample_type=SampleType.WGS,
                    crdq=crdq,
                ),
            )
        yield self.dynamic_crdq_tasks
