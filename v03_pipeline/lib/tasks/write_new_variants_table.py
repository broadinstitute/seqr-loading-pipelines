import math

import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.annotations.rdc_dependencies import (
    get_rdc_annotation_dependencies,
)
from v03_pipeline.lib.misc.allele_registry import register_alleles_in_chunks
from v03_pipeline.lib.misc.callsets import callset_project_pairs, get_callset_ht
from v03_pipeline.lib.misc.math import constrain
from v03_pipeline.lib.model import Env, ReferenceDatasetCollection, SampleType
from v03_pipeline.lib.paths import (
    new_variants_table_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.reference_data.gencode.mapping_gene_ids import (
    load_gencode_ensembl_to_refseq_id,
    load_gencode_gene_symbol_to_gene_id,
)
from v03_pipeline.lib.tasks.base.base_update_variant_annotations_table import (
    BaseUpdateVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.reference_data.update_variant_annotations_table_with_updated_reference_dataset import (
    UpdateVariantAnnotationsTableWithUpdatedReferenceDataset,
)
from v03_pipeline.lib.tasks.update_lookup_table import (
    UpdateLookupTableTask,
)
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)
from v03_pipeline.lib.vep import run_vep

VARIANTS_PER_VEP_PARTITION = 1e3
GENCODE_RELEASE = 42
GENCODE_FOR_VEP_RELEASE = 44


class WriteNewVariantsTableTask(BaseWriteTask):
    sample_type = luigi.EnumParameter(enum=SampleType)
    callset_paths = luigi.ListParameter()
    project_guids = luigi.ListParameter()
    project_remap_paths = luigi.ListParameter()
    project_pedigree_paths = luigi.ListParameter()
    imputed_sex_paths = luigi.ListParameter(default=None)
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
    liftover_ref_path = luigi.OptionalParameter(
        default='gs://hail-common/references/grch38_to_grch37.over.chain.gz',
        description='Path to GRCh38 to GRCh37 coordinates file',
    )
    run_id = luigi.Parameter()

    @property
    def annotation_dependencies(self) -> dict[str, hl.Table]:
        deps = get_rdc_annotation_dependencies(self.dataset_type, self.reference_genome)
        if self.dataset_type.has_gencode_ensembl_to_refseq_id_mapping(
            self.reference_genome,
        ):
            deps['gencode_ensembl_to_refseq_id_mapping'] = hl.literal(
                load_gencode_ensembl_to_refseq_id(GENCODE_FOR_VEP_RELEASE),
            )
        if self.dataset_type.has_gencode_gene_symbol_to_gene_id_mapping:
            deps['gencode_gene_symbol_to_gene_id_mapping'] = hl.literal(
                load_gencode_gene_symbol_to_gene_id(GENCODE_RELEASE, ''),
            )
        return deps

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            new_variants_table_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

    def requires(self) -> list[luigi.Task]:
        if Env.REFERENCE_DATA_AUTO_UPDATE:
            upstream_table_tasks = [
                UpdateVariantAnnotationsTableWithUpdatedReferenceDataset(
                    self.reference_genome,
                    self.dataset_type,
                ),
            ]
        else:
            upstream_table_tasks = [
                BaseUpdateVariantAnnotationsTableTask(
                    self.reference_genome,
                    self.dataset_type,
                ),
            ]
        if self.dataset_type.has_lookup_table:
            # NB: the lookup table task has remapped and subsetted callset tasks as dependencies.
            upstream_table_tasks.extend(
                [
                    UpdateLookupTableTask(
                        self.reference_genome,
                        self.dataset_type,
                        self.sample_type,
                        self.callset_paths,
                        self.project_guids,
                        self.project_remap_paths,
                        self.project_pedigree_paths,
                        self.imputed_sex_paths,
                        self.ignore_missing_samples_when_remapping,
                        self.validate,
                        self.force,
                    ),
                ],
            )
        else:
            upstream_table_tasks.extend(
                [
                    WriteRemappedAndSubsettedCallsetTask(
                        self.reference_genome,
                        self.dataset_type,
                        self.sample_type,
                        callset_path,
                        project_guid,
                        project_remap_path,
                        project_pedigree_path,
                        imputed_sex_path,
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
                ],
            )
        return upstream_table_tasks

    def complete(self) -> bool:
        return super().complete() and hl.eval(
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

    def create_table(self) -> hl.Table:
        callset_ht = get_callset_ht(
            self.reference_genome,
            self.dataset_type,
            self.callset_paths,
            self.project_guids,
            self.project_remap_paths,
            self.project_pedigree_paths,
            self.imputed_sex_paths,
        )

        # 1) Identify new variants.
        annotations_ht = hl.read_table(
            variant_annotations_table_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )
        new_variants_ht = callset_ht.anti_join(annotations_ht)

        # Annotate new variants with VEP.
        # Note about the repartition: our work here is cpu/memory bound and
        # proportional to the number of new variants.  Our default partitioning
        # will under-partition in that regard, so we split up our work
        # with a partitioning scheme local to this task.
        new_variants_count = new_variants_ht.count()
        new_variants_ht = new_variants_ht.repartition(
            constrain(
                math.ceil(new_variants_count / VARIANTS_PER_VEP_PARTITION),
                10,
                10000,
            ),
        )
        new_variants_ht = run_vep(
            new_variants_ht,
            self.dataset_type,
            self.reference_genome,
        )

        # Select down to the formatting annotations fields and
        # any reference dataset collection annotations.
        new_variants_ht = new_variants_ht.select(
            **get_fields(
                new_variants_ht,
                self.dataset_type.formatting_annotation_fns(self.reference_genome),
                **self.annotation_dependencies,
                **self.param_kwargs,
            ),
        )

        # Join new variants against the reference dataset collections that are not "annotated".
        for rdc in ReferenceDatasetCollection.for_reference_genome_dataset_type(
            self.reference_genome,
            self.dataset_type,
        ):
            if rdc.requires_annotation:
                continue
            rdc_ht = self.annotation_dependencies[f'{rdc.value}_ht']
            new_variants_ht = new_variants_ht.join(rdc_ht, 'left')

        # Register the new variant alleles to the Clingen Allele Registry
        # and annotate new_variants table with CAID.
        if (
            Env.SHOULD_REGISTER_ALLELES
            and self.dataset_type.should_send_to_allele_registry
        ):
            ar_ht = hl.Table.parallelize(
                [],
                hl.tstruct(
                    locus=hl.tlocus(self.reference_genome.value),
                    alleles=hl.tarray(hl.tstr),
                    CAID=hl.tstr,
                ),
                key=('locus', 'alleles'),
            )
            for ar_ht_chunk in register_alleles_in_chunks(
                new_variants_ht,
                self.reference_genome,
            ):
                ar_ht = ar_ht.union(ar_ht_chunk)
            new_variants_ht = new_variants_ht.join(ar_ht, 'left')

        return new_variants_ht.select_globals(
            updates={
                hl.Struct(callset=callset_path, project_guid=project_guid)
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
            },
        )
