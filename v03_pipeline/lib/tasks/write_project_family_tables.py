import functools

import luigi

from v03_pipeline.lib.misc.io import import_pedigree
from v03_pipeline.lib.misc.pedigree import parse_pedigree_ht_to_families
from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.write_family_table import WriteFamilyTableTask


class WriteProjectFamilyTablesTask(luigi.Task):
    reference_genome = luigi.EnumParameter(enum=ReferenceGenome)
    dataset_type = luigi.EnumParameter(enum=DatasetType)
    sample_type = luigi.EnumParameter(enum=SampleType)
    callset_path = luigi.Parameter()
    project_guid = luigi.ListParameter()
    project_remap_path = luigi.ListParameter()
    project_pedigree_path = luigi.ListParameter()
    ignore_missing_samples_when_subsetting = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    ignore_missing_samples_when_remapping = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    validate = luigi.BoolParameter(
        default=True,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    is_new_gcnv_joint_call = luigi.BoolParameter(
        default=False,
        description='Is this a fully joint-called callset.',
    )

    @functools.cached_property
    def write_family_table_tasks(self) -> list[luigi.Task]:
        pedigree_ht = import_pedigree(self.project_pedigree_path)
        families = parse_pedigree_ht_to_families(pedigree_ht)
        return [
            WriteFamilyTableTask(
                **self.param_kwargs,
                family_guid=family.family_guid,
            )
            for family in families
        ]

    def complete(self) -> bool:
        return all(
            write_family_table_task.complete()
            for write_family_table_task in self.write_family_table_tasks
        )

    def requires(self) -> luigi.Task:
        return self.write_family_table_tasks
