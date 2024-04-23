import hail as hl
import luigi

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.update_project_table import (
    UpdateProjectTableTask,
)
from v03_pipeline.lib.tasks.write_family_table import WriteFamilyTableTask


class WriteProjectFamilyTablesTask(luigi.Task):
    reference_genome = luigi.EnumParameter(enum=ReferenceGenome)
    dataset_type = luigi.EnumParameter(enum=DatasetType)
    sample_type = luigi.EnumParameter(enum=SampleType)
    callset_path = luigi.Parameter()
    project_guid = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()
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
    force = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    is_new_gcnv_joint_call = luigi.BoolParameter(
        default=False,
        description='Is this a fully joint-called callset.',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dynamic_write_family_table_tasks = set()

    def complete(self) -> bool:
        return (
            not self.force
            and len(self.dynamic_write_family_table_tasks) >= 1
            and all(
                write_family_table_task.complete()
                for write_family_table_task in self.dynamic_write_family_table_tasks
            )
        )

    def run(self):
        # https://luigi.readthedocs.io/en/stable/tasks.html#dynamic-dependencies
        update_project_table_task: luigi.Target = yield UpdateProjectTableTask(
            self.reference_genome,
            self.dataset_type,
            self.sample_type,
            self.project_guid,
            self.callset_path,
            self.project_remap_path,
            self.project_pedigree_path,
            self.ignore_missing_samples_when_subsetting,
            self.ignore_missing_samples_when_remapping,
            self.validate,
            False,
            self.is_new_gcnv_joint_call,
        )
        project_ht = hl.read_table(update_project_table_task.path)
        family_guids = hl.eval(project_ht.globals.family_guids)
        for family_guid in family_guids:
            self.dynamic_write_family_table_tasks.add(
                WriteFamilyTableTask(
                    **self.param_kwargs,
                    family_guid=family_guid,
                ),
            )
        yield self.dynamic_write_family_table_tasks
