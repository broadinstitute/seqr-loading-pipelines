import functools
import math

import hail as hl
import luigi

from v03_pipeline.lib.annotations.enums import annotate_enums
from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.model import ReferenceDatasetCollection
from v03_pipeline.lib.paths import (
    remapped_and_subsetted_callset_path,
    sample_lookup_table_path,
    valid_reference_dataset_collection_path,
)
from v03_pipeline.lib.reference_data.gencode.mapping_gene_ids import load_gencode
from v03_pipeline.lib.tasks.base.base_variant_annotations_table import (
    BaseVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.update_sample_lookup_table import (
    UpdateSampleLookupTableTask,
)
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)
from v03_pipeline.lib.vep import run_vep

GENCODE_RELEASE = 42
VARIANTS_PER_VEP_PARTITION = 20e3


class UpdateVariantAnnotationsTableWithNewSamplesTask(BaseVariantAnnotationsTableTask):
    callset_path = luigi.Parameter()
    project_guids = luigi.ListParameter()
    project_remap_paths = luigi.ListParameter()
    project_pedigree_paths = luigi.ListParameter()
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
    liftover_ref_path = luigi.OptionalParameter(
        default='gs://hail-common/references/grch38_to_grch37.over.chain.gz',
        description='Path to GRCh38 to GRCh37 coordinates file',
    )
    vep_config_json_path = luigi.OptionalParameter(
        default=None,
        description='Path of hail vep config .json file',
    )

    def read_annotation_dependencies(self):
        annotation_dependencies = {}

        for rdc in ReferenceDatasetCollection.for_dataset_type(self.dataset_type):
            annotation_dependencies[f'{rdc.value}_ht'] = hl.read_table(
                valid_reference_dataset_collection_path(
                    self.reference_genome,
                    self.dataset_type,
                    rdc,
                ),
            )

        if self.dataset_type.has_sample_lookup_table:
            annotation_dependencies['sample_lookup_ht'] = hl.read_table(
                sample_lookup_table_path(
                    self.reference_genome,
                    self.dataset_type,
                ),
            )

        if self.dataset_type.has_gencode_mapping:
            annotation_dependencies['gencode_mapping'] = hl.literal(
                load_gencode(GENCODE_RELEASE, ''),
            )

        return annotation_dependencies

    def requires(self) -> list[luigi.Task]:
        if self.dataset_type.has_sample_lookup_table:
            # NB: the sample lookup table task has remapped and subsetted callset tasks as dependencies.
            upstream_table_tasks = [
                UpdateSampleLookupTableTask(
                    self.reference_genome,
                    self.dataset_type,
                    self.sample_type,
                    self.callset_path,
                    self.project_guids,
                    self.project_remap_paths,
                    self.project_pedigree_paths,
                    self.ignore_missing_samples_when_subsetting,
                    self.ignore_missing_samples_when_remapping,
                    self.validate,
                ),
            ]
        else:
            upstream_table_tasks = [
                WriteRemappedAndSubsettedCallsetTask(
                    self.reference_genome,
                    self.dataset_type,
                    self.sample_type,
                    self.callset_path,
                    project_guid,
                    project_remap_path,
                    project_pedigree_path,
                    self.ignore_missing_samples_when_subsetting,
                    self.ignore_missing_samples_when_remapping,
                    self.validate,
                )
                for (project_guid, project_remap_path, project_pedigree_path) in zip(
                    self.project_guids,
                    self.project_remap_paths,
                    self.project_pedigree_paths,
                    strict=True,
                )
            ]
        return [
            *super().requires(),
            *upstream_table_tasks,
        ]

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.bind(
                lambda updates: hl.all(
                    [
                        updates.contains(
                            hl.Struct(
                                callset=self.callset_path,
                                project_guid=project_guid,
                            ),
                        )
                        for project_guid in self.project_guids
                    ],
                ),
                hl.read_table(self.output().path).updates,
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        callset_hts = [
            hl.read_matrix_table(
                remapped_and_subsetted_callset_path(
                    self.reference_genome,
                    self.dataset_type,
                    self.callset_path,
                    project_guid,
                ),
            ).rows()
            for project_guid in self.project_guids
        ]
        callset_ht = functools.reduce(
            (lambda ht1, ht2: ht1.union(ht2, unify=True)),
            callset_hts,
        )
        callset_ht = callset_ht.distinct()
        annotation_dependencies = self.read_annotation_dependencies()

        # 1) Get new rows and annotate with vep
        # Note about the repartition: our work here is cpu/memory bound and
        # proportional to the number of new variants.  Our default partitioning
        # will under-partition in that regard, so we split up our work
        # with a partitioning scheme local to this task.
        new_variants_ht = callset_ht.anti_join(ht)
        new_variants_count = new_variants_ht.count()
        new_variants_ht = new_variants_ht.repartition(
            max(math.ceil(new_variants_count / VARIANTS_PER_VEP_PARTITION), 1),
        )
        new_variants_ht = run_vep(
            new_variants_ht,
            self.dataset_type,
            self.vep_config_json_path,
        )

        # 2) Select down to the formatting annotations fields and
        # any reference dataset collection annotations.
        new_variants_ht = new_variants_ht.select(
            **get_fields(
                new_variants_ht,
                self.dataset_type.formatting_annotation_fns,
                **annotation_dependencies,
                **self.param_kwargs,
            ),
        )

        # 3) Join against the reference dataset collections that are not "annotated".
        for rdc in ReferenceDatasetCollection.for_dataset_type(self.dataset_type):
            if rdc.requires_annotation:
                continue
            rdc_ht = annotation_dependencies[f'{rdc.value}_ht']
            new_variants_ht = new_variants_ht.join(rdc_ht, 'left')

        # 4) Union with the existing variant annotations table
        # and annotate with the sample lookup table.
        ht = ht.union(new_variants_ht, unify=True)
        if self.dataset_type.has_sample_lookup_table:
            ht = ht.annotate(
                **get_fields(
                    ht,
                    self.dataset_type.sample_lookup_table_annotation_fns,
                    **annotation_dependencies,
                    **self.param_kwargs,
                ),
            )

        # 5) Fix up the globals.
        ht = ht.annotate_globals(
            paths=hl.Struct(),
            versions=hl.Struct(),
            enums=hl.Struct(),
        )
        for rdc in ReferenceDatasetCollection.for_dataset_type(self.dataset_type):
            rdc_ht = annotation_dependencies[f'{rdc.value}_ht']
            rdc_globals = rdc_ht.index_globals()
            ht = ht.annotate_globals(
                paths=hl.Struct(
                    **ht.globals.paths,
                    **rdc_globals.paths,
                ),
                versions=hl.Struct(
                    **ht.globals.versions,
                    **rdc_globals.versions,
                ),
                enums=hl.Struct(
                    **ht.globals.enums,
                    **rdc_globals.enums,
                ),
            )
        ht = annotate_enums(ht, self.dataset_type)

        # 6) Mark the table as updated with these callset/project pairs.
        return ht.annotate_globals(
            updates=ht.updates.union(
                {
                    hl.Struct(callset=self.callset_path, project_guid=project_guid)
                    for project_guid in self.project_guids
                },
            ),
        )
