import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.misc.io import import_pedigree
from v03_pipeline.lib.misc.pedigree import parse_pedigree_ht_to_families
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.files import RawFileTask
from v03_pipeline.lib.tasks.update_project_table import UpdateProjectTableTask
from v03_pipeline.lib.tasks.write_family_table import WriteFamilyTableTask


@luigi.util.inherits(BaseLoadingRunParams)
class WriteProjectFamilyTablesTask(luigi.Task):
    project_guid = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()

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
        # Fetch family guids from project table
        update_project_table_task: luigi.Target = yield self.clone(
            UpdateProjectTableTask,
            force=False,
        )
        project_ht = hl.read_table(update_project_table_task.path)
        family_guids_in_project_table = set(hl.eval(project_ht.globals.family_guids))

        # Fetch family guids from pedigree
        pedigree_ht_task: luigi.Target = yield RawFileTask(self.project_pedigree_path)
        pedigree_ht = import_pedigree(pedigree_ht_task.path)
        families_guids_in_pedigree = {
            f.family_guid for f in parse_pedigree_ht_to_families(pedigree_ht)
        }

        # Intersect them
        family_guids_to_load = (
            family_guids_in_project_table & families_guids_in_pedigree
        )
        for family_guid in family_guids_to_load:
            self.dynamic_write_family_table_tasks.add(
                self.clone(WriteFamilyTableTask, family_guid=family_guid),
            )
        yield self.dynamic_write_family_table_tasks
