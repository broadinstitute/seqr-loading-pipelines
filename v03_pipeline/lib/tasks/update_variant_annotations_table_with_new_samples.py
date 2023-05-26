from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.lib.annotations import annotate_all
from v03_pipeline.lib.definitions import SampleFileType, SampleType
from v03_pipeline.lib.misc.io import import_callset, import_pedigree, import_remap
from v03_pipeline.lib.misc.pedigree import samples_to_include
from v03_pipeline.lib.misc.sample_ids import (
    remap_sample_ids,
    subset_samples_and_variants,
)
from v03_pipeline.lib.tasks.base.base_variant_annotations_table import (
    BaseVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.files import RawFileTask, VCFFileTask


class UpdateVariantAnnotationsTableWithNewSamples(BaseVariantAnnotationsTableTask):
    sample_type = luigi.EnumParameter(enum=SampleType)
    callset_path = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()

    dont_validate = luigi.BoolParameter(
        default=False,
        description='Disable checking whether the dataset matches the specified sample type and genome version',
    )
    ignore_missing_samples = luigi.BoolParameter(default=False)
    liftover_ref_path = luigi.OptionalParameter(
        default='gs://hail-common/references/grch38_to_grch37.over.chain.gz',
        description='Path to GRCh38 to GRCh37 coordinates file',
    )
    vep_config_json_path = luigi.OptionalParameter(
        default=None,
        description='Path of hail vep config .json file',
    )

    def requires(self) -> list[luigi.Task]:
        return [
            VCFFileTask(self.callset_path)
            if self.dataset_type.sample_file_type == SampleFileType.VCF
            else RawFileTask(self.callset_path),
            RawFileTask(self.project_remap_path),
            RawFileTask(self.project_pedigree_path),
        ]

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.read_table(self.output().path).updates.contains(
                (self.callset_path, self.project_pedigree_path),
            ),
        )

    def update(self, existing_ht: hl.Table) -> hl.Table:
        # Import required files.
        callset_mt = import_callset(
            self.callset_path,
            self.reference_genome,
            self.dataset_type,
        )
        project_remap_ht = import_remap(self.project_remap_path)
        pedigree_ht = import_pedigree(self.project_pedigree_path)

        # Remap, then subset to samples & variants of interest.
        callset_mt = remap_sample_ids(callset_mt, project_remap_ht)
        sample_subset_ht = samples_to_include(pedigree_ht, callset_mt.cols())
        callset_mt = subset_samples_and_variants(
            callset_mt,
            sample_subset_ht,
            self.ignore_missing_samples,
        )

        # Split multi alleles
        callset_mt = hl.split_multi_hts(
            callset_mt.annotate_rows(
                locus_old=callset_mt.locus,
                alleles_old=callset_mt.alleles,
            ),
        )

        # Get new rows, annotate them, then stack onto the existing
        # variant annotations table.
        new_variants_ht = callset_mt.anti_join_rows(existing_ht).rows()
        new_variants_ht = annotate_all(new_variants_ht, **self.param_kwargs)
        unioned_ht = existing_ht.union(new_variants_ht, unify=True)
        return unioned_ht.annotate_globals(
            updates=unioned_ht.updates.add(
                (self.callset_path, self.project_pedigree_path),
            ),
        )
