import luigi
import luigi.util

from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams


@luigi.util.inherits(BaseLoadingRunParams)
class BaseLoadingRunWithProjectInfoParams(luigi.Task):
    project_guids = luigi.ListParameter()
    project_remap_paths = luigi.ListParameter()
    project_pedigree_paths = luigi.ListParameter()
