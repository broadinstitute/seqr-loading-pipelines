import math

import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.allele_registry import register_alleles_in_chunks
from v03_pipeline.lib.misc.callsets import get_callset_ht
from v03_pipeline.lib.misc.io import remap_pedigree_hash
from v03_pipeline.lib.misc.math import constrain
from v03_pipeline.lib.model import (
    Env,
)
from v03_pipeline.lib.paths import (
    new_variants_table_path,
    valid_reference_dataset_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.reference_datasets.gencode.mapping_gene_ids import (
    load_gencode_ensembl_to_refseq_id,
    load_gencode_gene_symbol_to_gene_id,
)
from v03_pipeline.lib.reference_datasets.reference_dataset import BaseReferenceDataset
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.reference_data.update_variant_annotations_table_with_updated_reference_dataset import (
    UpdateVariantAnnotationsTableWithUpdatedReferenceDataset,
)
from v03_pipeline.lib.tasks.update_lookup_table import (
    UpdateLookupTableTask,
)
from v03_pipeline.lib.tasks.write_metadata_for_run import (
    WriteMetadataForRunTask,
)
from v03_pipeline.lib.vep import run_vep

VARIANTS_PER_VEP_PARTITION = 1e3
GENCODE_RELEASE = 42
GENCODE_FOR_VEP_RELEASE = 44


@luigi.util.inherits(BaseLoadingRunParams)
class WriteNewVariantsTableTask(BaseWriteTask):
    @property
    def annotation_dependencies(self) -> dict[str, hl.Table]:
        deps = {}
        for (
            reference_dataset
        ) in BaseReferenceDataset.for_reference_genome_dataset_type_annotations(
            self.reference_genome,
            self.dataset_type,
        ):
            deps[f'{reference_dataset.value}_ht'] = hl.read_table(
                valid_reference_dataset_path(self.reference_genome, reference_dataset),
            )

        if self.dataset_type.has_gencode_ensembl_to_refseq_id_mapping(
            self.reference_genome,
        ):
            deps['gencode_ensembl_to_refseq_id_mapping'] = hl.literal(
                load_gencode_ensembl_to_refseq_id(GENCODE_FOR_VEP_RELEASE),
            )
        if self.dataset_type.has_gencode_gene_symbol_to_gene_id_mapping:
            deps['gencode_gene_symbol_to_gene_id_mapping'] = hl.literal(
                load_gencode_gene_symbol_to_gene_id(GENCODE_RELEASE),
            )
        deps[
            'grch37_to_grch38_liftover_ref_path'
        ] = Env.GRCH37_TO_GRCH38_LIFTOVER_REF_PATH
        deps[
            'grch38_to_grch37_liftover_ref_path'
        ] = Env.GRCH38_TO_GRCH37_LIFTOVER_REF_PATH
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
        requirements = [
            self.clone(UpdateVariantAnnotationsTableWithUpdatedReferenceDataset),
        ]
        if self.dataset_type.has_lookup_table:
            # NB: the lookup table task has remapped and subsetted callset tasks as dependencies.
            requirements = [
                *requirements,
                self.clone(UpdateLookupTableTask),
            ]
        else:
            requirements = [
                *requirements,
                self.clone(WriteMetadataForRunTask),
            ]
        return requirements

    def complete(self) -> bool:
        return super().complete() and hl.eval(
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

    def create_table(self) -> hl.Table:
        callset_ht = get_callset_ht(
            self.reference_genome,
            self.dataset_type,
            self.callset_path,
            self.project_guids,
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

        # Join new variants against the reference datasets that are not "annotated".
        for (
            reference_dataset
        ) in BaseReferenceDataset.for_reference_genome_dataset_type_annotations(
            self.reference_genome,
            self.dataset_type,
        ):
            if reference_dataset.is_keyed_by_interval:
                continue
            reference_dataset_ht = self.annotation_dependencies[
                f'{reference_dataset.value}_ht'
            ]
            reference_dataset_ht = reference_dataset_ht.select(
                **{
                    f'{reference_dataset.name}': hl.Struct(
                        **reference_dataset_ht.row_value,
                    ),
                },
            )
            new_variants_ht = new_variants_ht.join(reference_dataset_ht, 'left')

        # Register the new variant alleles to the Clingen Allele Registry
        # and annotate new_variants table with CAID.
        if (
            Env.CLINGEN_ALLELE_REGISTRY_LOGIN
            and Env.CLINGEN_ALLELE_REGISTRY_PASSWORD
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
        elif self.dataset_type.should_send_to_allele_registry:
            new_variants_ht = new_variants_ht.annotate(CAID=hl.missing(hl.tstr))

        return new_variants_ht.select_globals(
            updates={
                hl.Struct(
                    callset=self.callset_path,
                    project_guid=project_guid,
                    remap_pedigree_hash=remap_pedigree_hash(
                        self.project_remap_paths[i],
                        self.project_pedigree_paths[i],
                    ),
                )
                for i, project_guid in enumerate(self.project_guids)
            },
        )
