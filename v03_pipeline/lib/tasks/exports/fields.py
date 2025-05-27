import hail as hl

from v03_pipeline.lib.annotations.expression_helpers import get_expr_for_xpos
from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset
from v03_pipeline.lib.tasks.exports.misc import array_structexpression_fields


def reference_independent_contig(locus: hl.LocusExpression):
    return locus.contig.replace('^chr', '').replace('MT', 'M')


def get_entries_export_fields(
    ht: hl.Table,
    dataset_type: DatasetType,
    sample_type: SampleType,
    project_guid: str,
):
    return {
        'key_': ht.key_,
        'project_guid': project_guid,
        'family_guid': ht.family_entries.family_guid[0],
        'sample_type': sample_type.value,
        'xpos': get_expr_for_xpos(ht.locus),
        **(
            {
                'is_gnomad_gt_5_percent': hl.is_defined(ht.is_gt_5_percent),
            }
            if hasattr(ht, 'is_gt_5_percent')
            else {}
        ),
        'filters': ht.filters,
        'calls': ht.family_entries.map(
            lambda fe: dataset_type.calls_export_fields(fe),
        ),
        'sign': 1,
    }


def get_predictions_export_fields(
    ht: hl.Table,
    _dataset_type: DatasetType,
):
    return {
        'cadd': ht.dbnsfp.CADD_phred,
        'eigen': ht.eigen.Eigen_phred,
        'fathmm': ht.dbnsfp.fathmm_MKL_coding_score,
        **(
            {
                'gnomad_noncoding': ht.gnomad_non_coding_constraint.z_score,
            }
            if hasattr(ht, 'gnomad_non_coding_constraint')
            else {}
        ),
        'mpc': ht.dbnsfp.MPC_score,
        'mut_pred': ht.dbnsfp.MutPred_score,
        'mut_tester': ht.dbnsfp.MutationTaster_pred,
        'polyphen': ht.dbnsfp.Polyphen2_HVAR_score,
        'primate_ai': ht.dbnsfp.PrimateAI_score,
        'revel': ht.dbnsfp.REVEL_score,
        'sift': ht.dbnsfp.SIFT_score,
        'splice_ai': ht.splice_ai.delta_score,
        'splice_ai_consequence': ht.splice_ai.splice_consequence,
        'vest': ht.dbnsfp.VEST4_score,
    }


def get_variants_export_fields(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
):
    return {
        'key_': ht.key_,
        'xpos': ht.xpos,
        'chrom': reference_independent_contig(ht.locus),
        'pos': ht.locus.position,
        'ref': ht.alleles[0],
        'alt': ht.alleles[1],
        'variantId': ht.variant_id,
        'rsid': ht.rsid,
        **(
            {
                'CAID': ht.CAID,
            }
            if hasattr(ht, 'CAID')
            else {}
        ),
        'liftedOverChrom': (
            reference_independent_contig(ht.rg37_locus)
            if hasattr(ht, 'rg37_locus')
            else reference_independent_contig(ht.rg38_locus)
        ),
        'liftedOverPos': (
            reference_independent_contig(ht.rg37_locus)
            if hasattr(ht, 'rg37_locus')
            else reference_independent_contig(ht.rg38_locus)
        ),
        **(
            {
                'hgmd': ht.hgmd
                if hasattr(ht, 'hgmd')
                else hl.missing(hl.tstruct(accession=hl.tstr, class_=hl.tstr)),
            }
            if dataset_type in ReferenceDataset.hgmd.dataset_types(reference_genome)
            else {}
        ),
        **(
            {
                'screenRegionType': ht.screen.region_types.first(),
            }
            if hasattr(ht, 'screen')
            else {}
        ),
        'predictions': hl.Struct(**get_predictions_export_fields(ht, dataset_type)),
        'populations': hl.Struct(
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
    }
