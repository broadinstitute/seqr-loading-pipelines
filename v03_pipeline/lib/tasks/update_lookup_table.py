import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.misc.lookup import (
    compute_callset_lookup_ht,
    join_lookup_hts,
    remove_family_guids,
)
from v03_pipeline.lib.model.constants import PROJECTS_EXCLUDED_FROM_LOOKUP
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_update_lookup_table import (
    BaseUpdateLookupTableTask,
)
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


@luigi.util.inherits(BaseLoadingRunParams)
class UpdateLookupTableTask(BaseUpdateLookupTableTask):
    project_guids = luigi.ListParameter()
    project_remap_paths = luigi.ListParameter()
    project_pedigree_paths = luigi.ListParameter()

    def complete(self) -> bool:
        return (
            not self.force
            and super().complete()
            and hl.eval(
                hl.bind(
                    lambda updates: hl.all(
                        [
                            updates.contains(
                                hl.Struct(
                                    callset=self.callset_path,
                                    project_guid=project_guid,
                                    remap_pedigree_hash=remap_pedigree_hash(
                                        self.project_remap_paths[i],
                                        self.project_pedigree_paths[i],
                                    ),
                                ),
                            )
                            for i, project_guid in enumerate(self.project_guids)
                        ],
                    ),
                    hl.read_table(self.output().path).updates,
                ),
            )
        )

    def requires(self) -> list[luigi.Task]:
        return [
            self.clone(
                WriteRemappedAndSubsettedCallsetTask,
                project_guid=project_guid,
                project_remap_path=project_remap_path,
                project_pedigree_path=project_pedigree_path,
                force=False,
            )
            for (
                project_guid,
                project_remap_path,
                project_pedigree_path,
            ) in zip(
                self.project_guids,
                self.project_remap_paths,
                self.project_pedigree_paths,
                strict=True,
            )
        ]

    def update_table(self, ht: hl.Table) -> hl.Table:
        # NB: there's a chance this many hail operations blows the DAG compute stack
        # in an unfortunate way.  Please keep an eye out!
        for i, project_guid in enumerate(self.project_guids):
            if project_guid in PROJECTS_EXCLUDED_FROM_LOOKUP:
                ht = ht.annotate_globals(
                    updates=ht.updates.add(
                        hl.Struct(
                            callset=self.callset_path,
                            project_guid=project_guid,
                            remap_pedigree_hash=hl.missing(hl.tstr),
                        ),
                    ),
                )
                continue
            callset_mt = hl.read_matrix_table(self.input()[i].path)
            ht = remove_family_guids(
                ht,
                project_guid,
                callset_mt.index_globals().family_samples.key_set(),
            )
            callset_ht = compute_callset_lookup_ht(
                self.dataset_type,
                callset_mt,
                project_guid,
            )
            ht = join_lookup_hts(
                ht,
                callset_ht,
            )
            ht = ht.select_globals(
                project_guids=ht.project_guids,
                project_families=ht.project_families,
                updates=ht.updates.add(
                    hl.Struct(
                        callset=self.callset_path,
                        project_guid=project_guid,
                        remap_pedigree_hash=remap_pedigree_hash(
                            self.project_remap_paths[i],
                            self.project_pedigree_paths[i],
                        ),
                    ),
                ),
            )
        return ht
