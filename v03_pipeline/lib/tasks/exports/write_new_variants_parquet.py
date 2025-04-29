import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.paths import (
    new_variants_parquet_path,
    new_variants_table_path,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.base.base_write_parquet import BaseWriteParquetTask
from v03_pipeline.lib.tasks.exports.misc import (
    array_structexpression_fields,
    camelcase_array_structexpression_fields,
    subset_filterable_transcripts_fields,
    unmap_formatting_annotation_enums,
    unmap_reference_dataset_annotation_enums,
)
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.update_new_variants_with_caids import (
    UpdateNewVariantsWithCAIDsTask,
)
from v03_pipeline.lib.tasks.write_new_variants_table import WriteNewVariantsTableTask


@luigi.util.inherits(BaseLoadingRunParams)
class WriteNewVariantsParquetTask(BaseWriteParquetTask):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            new_variants_parquet_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

    def requires(self) -> list[luigi.Task]:
        return [
            self.clone(UpdateNewVariantsWithCAIDsTask)
            if self.dataset_type.should_send_to_allele_registry
            else self.clone(WriteNewVariantsTableTask),
        ]

    def create_table(self) -> None:
        ht = hl.read_table(
            new_variants_table_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )
        ht = unmap_formatting_annotation_enums(
            ht,
            self.reference_genome,
            self.dataset_type,
        )
        ht = unmap_reference_dataset_annotation_enums(
            ht,
            self.reference_genome,
            self.dataset_type,
        )
        ht = camelcase_array_structexpression_fields(ht, self.reference_genome)
        ht = subset_filterable_transcripts_fields(
            ht,
            self.reference_genome,
            self.dataset_type,
        )
        ht = ht.key_by()
        return ht.select(
            key_=ht.key_,
            xpos=ht.xpos,
            chrom=ht.locus.contig.replace('^chr', ''),
            pos=ht.locus.position,
            ref=ht.alleles[0],
            alt=ht.alleles[1],
            variantId=ht.variant_id,
            rsid=ht.rsid,
            CAID=ht.CAID,
            liftedOverChrom=ht.rg37_locus.contig,
            liftedOverPos=ht.rg37_locus.position,
            hgmd=(
                ht.hgmd
                if hasattr(ht, 'hgmd')
                else hl.missing(hl.tstruct(accession=hl.tstr, class_=hl.tstr))
            ),
            screenRegionType=ht.screen.region_types.first(),
            predictions=hl.Struct(
                cadd=ht.dbnsfp.CADD_phred,
                eigen=ht.eigen.Eigen_phred,
                fathmm=ht.dbnsfp.fathmm_MKL_coding_score,
                gnomad_noncoding=ht.gnomad_non_coding_constraint.z_score,
                mpc=ht.dbnsfp.MPC_score,
                mut_pred=ht.dbnsfp.MutPred_score,
                mut_tester=ht.dbnsfp.MutationTaster_pred,
                polyphen=ht.dbnsfp.Polyphen2_HVAR_score,
                primate_ai=ht.dbnsfp.PrimateAI_score,
                revel=ht.dbnsfp.REVEL_score,
                sift=ht.dbnsfp.SIFT_score,
                splice_ai=ht.splice_ai.delta_score,
                splice_ai_consequence=ht.splice_ai.splice_consequence,
                vest=ht.dbnsfp.VEST4_score,
            ),
            populations=hl.Struct(
                exac=hl.Struct(
                    ac=ht.exac.AC_Adj,
                    af=ht.exac.AF,
                    an=ht.exac.AN_Adj,
                    filter_af=ht.exac.AF_POPMAX,
                    hemi=ht.exac.AC_Hemi,
                    het=ht.exac.AC_Het,
                    hom=ht.exac.AC_Hom,
                ),
                gnomad_exomes=hl.Struct(
                    ac=ht.gnomad_exomes.AC,
                    af=ht.gnomad_exomes.AF,
                    an=ht.gnomad_exomes.AN,
                    filter_af=ht.gnomad_exomes.AF_POPMAX_OR_GLOBAL,
                    hemi=ht.gnomad_exomes.Hemi,
                    hom=ht.gnomad_exomes.Hom,
                ),
                gnomad_genomes=hl.Struct(
                    ac=ht.gnomad_genomes.AC,
                    af=ht.gnomad_genomes.AF,
                    an=ht.gnomad_genomes.AN,
                    filter_af=ht.gnomad_genomes.AF_POPMAX_OR_GLOBAL,
                    hemi=ht.gnomad_genomes.Hemi,
                    hom=ht.gnomad_genomes.Hom,
                ),
                topmed=hl.Struct(
                    ac=ht.topmed.AC,
                    af=ht.topmed.AF,
                    an=ht.topmed.AN,
                    het=ht.topmed.Het,
                    hom=ht.topmed.Hom,
                ),
            ),
            **{f: ht[f] for f in sorted(array_structexpression_fields(ht))},
        )
