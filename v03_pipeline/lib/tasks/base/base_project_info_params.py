import luigi
import luigi.util


class BaseProjectInfoParams(luigi.Task):
    project_guids = luigi.ListParameter()
    project_remap_paths = luigi.ListParameter()
    project_pedigree_paths = luigi.ListParameter()
    run_id = luigi.Parameter()
