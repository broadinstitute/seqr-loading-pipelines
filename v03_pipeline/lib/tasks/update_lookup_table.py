import hail as hl
import luigi

from v03_pipeline.lib.misc.callsets import callset_project_pairs
from v03_pipeline.lib.misc.lookup import (
    compute_callset_lookup_ht,
    join_lookup_hts,
    remove_family_guids,
)
from v03_pipeline.lib.model.constants import PROJECTS_EXCLUDED_FROM_LOOKUP
from v03_pipeline.lib.tasks.base.base_update_lookup_table import (
    BaseUpdateLookupTableTask,
)
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


class UpdateLookupTableTask(BaseUpdateLookupTableTask):
    callset_paths = luigi.ListParameter()
    project_guids = luigi.ListParameter()
    project_remap_paths = luigi.ListParameter()
    project_pedigree_paths = luigi.ListParameter()
    imputed_sex_paths = luigi.ListParameter(default=None)
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
                                    callset=callset_path,
                                    project_guid=project_guid,
                                ),
                            )
                            for (
                                callset_path,
                                project_guid,
                                _,
                                _,
                                _,
                            ) in callset_project_pairs(
                                self.callset_paths,
                                self.project_guids,
                                self.project_remap_paths,
                                self.project_pedigree_paths,
                                self.imputed_sex_paths,
                            )
                        ],
                    ),
                    hl.read_table(self.output().path).updates,
                ),
            )
        )

    def requires(self) -> list[luigi.Task]:
        return [
            WriteRemappedAndSubsettedCallsetTask(
                self.reference_genome,
                self.dataset_type,
                self.sample_type,
                callset_path,
                project_guid,
                project_remap_path,
                project_pedigree_path,
                imputed_sex_path,
                self.ignore_missing_samples_when_subsetting,
                self.ignore_missing_samples_when_remapping,
                self.validate,
                False,
            )
            for (
                callset_path,
                project_guid,
                project_remap_path,
                project_pedigree_path,
                imputed_sex_path,
            ) in callset_project_pairs(
                self.callset_paths,
                self.project_guids,
                self.project_remap_paths,
                self.project_pedigree_paths,
                self.imputed_sex_paths,
            )
        ]

    def update_table(self, ht: hl.Table) -> hl.Table:
        # NB: there's a chance this many hail operations blows the DAG compute stack
        # in an unfortunate way.  Please keep an eye out!
        for i, (callset_path, project_guid, _, _) in enumerate(
            callset_project_pairs(
                self.callset_paths,
                self.project_guids,
                self.project_remap_paths,
                self.project_pedigree_paths,
            ),
        ):
            if project_guid in PROJECTS_EXCLUDED_FROM_LOOKUP:
                ht = ht.annotate_globals(
                    updates=ht.updates.add(
                        hl.Struct(
                            callset=callset_path,
                            project_guid=project_guid,
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
                        callset=callset_path,
                        project_guid=project_guid,
                    ),
                ),
            )
        return ht
