import luigi

from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
)
from v03_pipeline.lib.tasks.migrate_variant_annotations_table import MigrateVariantAnnotationsTableTask


class MigrateAllVariantAnnotationsTablesTask(luigi.Task):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dynamic_migrate_tasks = list()

    def complete(self) -> bool:
        return self.checked_for_tasks

    def run(self):
        self.checked_for_tasks = True
        for crdq in CachedReferenceDatasetQuery.for_reference_genome_dataset_type(
            self.reference_genome,
            self.dataset_type,
        ):
            self.dynamic_crdq_tasks.add(
                UpdatedCachedReferenceDatasetQuery(
                    **self.param_kwargs,
                    crdq=crdq,
                ),
            )
        yield self.dynamic_crdq_tasks
