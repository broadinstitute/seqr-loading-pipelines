import hail as hl

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.exports.misc import reformat_transcripts_for_export

STANDARD_CONTIGS = hl.set(
    [c.replace('MT', 'M') for c in ReferenceGenome.GRCh37.standard_contigs],
)


def reference_independent_contig(locus: hl.LocusExpression):
    contig = locus.contig.replace('^chr', '').replace('MT', 'M')
    return hl.or_missing(
        # lifted over alternate contigs may be present
        # even though the primary contig is filtered to
        # standard contigs earlier in the pipeline
        STANDARD_CONTIGS.contains(contig),
        contig,
    )


def get_dataset_type_specific_annotations(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
):
    return {
        DatasetType.SNV_INDEL: lambda ht: {
            'hgmd': (
                ht.hgmd
                if hasattr(ht, 'hgmd')
                else hl.missing(hl.tstruct(accession=hl.tstr, classification=hl.tstr))
            ),
            **(
                {'screenRegionType': ht.screen.region_types.first()}
                if reference_genome == ReferenceGenome.GRCh38
                else {}
            ),
        },
        DatasetType.MITO: lambda ht: {
            'commonLowHeteroplasmy': ht.common_low_heteroplasmy,
            'mitomapPathogenic': ht.mitomap.pathogenic,
        },
        DatasetType.SV: lambda ht: {
            'algorithms': ht.algorithms,
            'bothsidesSupport': ht.bothsides_support,
            'cpxIntervals': ht.cpxIntervals.map(
                lambda cpx_i: hl.Struct(
                    chrom=reference_independent_contig(cpx_i.start),
                    start=cpx_i.start.position,
                    end=cpx_i.end.position,
                    type=cpx_i.type,
                ),
            ),
            'endChrom': hl.or_missing(
                (
                    (ht.sv_type != 'INS')
                    & (ht.start_locus.contig != ht.end_locus.contig)
                ),
                reference_independent_contig(ht.end_locus),
            ),
            'svSourceDetail': hl.or_missing(
                (
                    (ht.sv_type == 'INS')
                    & (ht.start_locus.contig != ht.end_locus.contig)
                ),
                hl.Struct(chrom=reference_independent_contig(ht.end_locus)),
            ),
            'svType': ht.sv_type,
            'svTypeDetail': ht.sv_type_detail,
        },
        DatasetType.GCNV: lambda ht: {
            'numExon': ht.num_exon,
            'svType': ht.sv_type,
        },
    }[dataset_type](ht)


def get_calls_export_fields(
    ht: hl.Table,
    fe: hl.Struct,
    dataset_type: DatasetType,
):
    return {
        DatasetType.SNV_INDEL: lambda fe: hl.Struct(
            sampleId=fe.s,
            gt=fe.GT.n_alt_alleles(),
            gq=fe.GQ,
            ab=fe.AB,
            dp=fe.DP,
        ),
        DatasetType.MITO: lambda fe: hl.Struct(
            sampleId=fe.s,
            gt=fe.GT.n_alt_alleles(),
            dp=fe.DP,
            hl=fe.HL,
            mitoCn=fe.mito_cn,
            contamination=fe.contamination,
        ),
        DatasetType.SV: lambda fe: hl.Struct(
            sampleId=fe.s,
            gt=fe.GT.n_alt_alleles(),
            cn=fe.CN,
            gq=fe.GQ,
            newCall=fe.concordance.new_call,
            prevCall=fe.concordance.prev_call,
            prevNumAlt=fe.concordance.prev_num_alt,
        ),
        DatasetType.GCNV: lambda fe: hl.Struct(
            sampleId=fe.s,
            gt=fe.GT.n_alt_alleles(),
            cn=fe.CN,
            qs=fe.QS,
            defragged=fe.defragged,
            start=hl.or_else(fe.sample_start, ht.start_locus.position),
            end=hl.or_else(fe.sample_end, ht.end_locus.position),
            numExon=hl.or_else(fe.sample_num_exon, ht.num_exon),
            geneIds=hl.or_else(
                fe.sample_gene_ids,
                hl.set(ht.sorted_gene_consequences.gene_id),
            ),
            newCall=fe.concordance.new_call,
            prevCall=fe.concordance.prev_call,
            prevOverlap=fe.concordance.prev_overlap,
        ),
    }[dataset_type](fe)


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
        **(
            {
                'sample_type': sample_type.value,
            }
            if dataset_type in {DatasetType.SNV_INDEL, DatasetType.MITO}
            else {}
        ),
        'xpos': ht.xpos,
        **(
            {
                'is_gnomad_gt_5_percent': hl.is_defined(ht.is_gt_5_percent),
            }
            if hasattr(ht, 'is_gt_5_percent')
            else {}
        ),
        'filters': ht.filters,
        'calls': ht.family_entries.map(
            lambda fe: get_calls_export_fields(ht, fe, dataset_type),
        ),
        'sign': 1,
    }


def get_predictions_export_fields(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
):
    return {
        DatasetType.SNV_INDEL: lambda ht: {
            'cadd': ht.dbnsfp.CADD_phred,
            'eigen': ht.eigen.Eigen_phred,
            'fathmm': ht.dbnsfp.fathmm_MKL_coding_score,
            **(
                {
                    'gnomad_noncoding': ht.gnomad_non_coding_constraint.z_score,
                }
                if reference_genome == ReferenceGenome.GRCh38
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
        },
        DatasetType.MITO: lambda ht: {
            'apogee': ht.mitimpact.score,
            'haplogroup_defining': ht.haplogroup.is_defining,
            'hmtvar': ht.hmtvar.score,
            'mitotip': ht.mitotip.trna_prediction,
            'mut_taster': ht.dbnsfp.MutationTaster_pred,
            'sift': ht.dbnsfp.SIFT_score,
            'mlc': ht.local_constraint_mito.score,
        },
        DatasetType.SV: lambda ht: {
            'strvctvre': ht.strvctvre.score,
        },
        DatasetType.GCNV: lambda ht: {
            'strvctvre': ht.strvctvre.score,
        },
    }[dataset_type](ht)


def get_populations_export_fields(ht: hl.Table, dataset_type: DatasetType):
    return {
        DatasetType.SNV_INDEL: lambda ht: {
            'exac': hl.Struct(
                ac=ht.exac.AC_Adj,
                af=ht.exac.AF,
                an=ht.exac.AN_Adj,
                filter_af=ht.exac.AF_POPMAX,
                hemi=ht.exac.AC_Hemi,
                het=ht.exac.AC_Het,
                hom=ht.exac.AC_Hom,
            ),
            'gnomad_exomes': hl.Struct(
                ac=ht.gnomad_exomes.AC,
                af=ht.gnomad_exomes.AF,
                an=ht.gnomad_exomes.AN,
                filter_af=ht.gnomad_exomes.AF_POPMAX_OR_GLOBAL,
                hemi=ht.gnomad_exomes.Hemi,
                hom=ht.gnomad_exomes.Hom,
            ),
            'gnomad_genomes': hl.Struct(
                ac=ht.gnomad_genomes.AC,
                af=ht.gnomad_genomes.AF,
                an=ht.gnomad_genomes.AN,
                filter_af=ht.gnomad_genomes.AF_POPMAX_OR_GLOBAL,
                hemi=ht.gnomad_genomes.Hemi,
                hom=ht.gnomad_genomes.Hom,
            ),
            'topmed': hl.Struct(
                ac=ht.topmed.AC,
                af=ht.topmed.AF,
                an=ht.topmed.AN,
                het=ht.topmed.Het,
                hom=ht.topmed.Hom,
            ),
        },
        DatasetType.MITO: lambda ht: {
            'gnomad_mito': hl.Struct(
                ac=ht.gnomad_mito.AC_hom,
                af=ht.gnomad_mito.AF_hom,
                an=ht.gnomad_mito.AN,
            ),
            'gnomad_mito_heteroplasmy': hl.Struct(
                ac=ht.gnomad_mito.AC_het,
                af=ht.gnomad_mito.AF_hom,
                an=ht.gnomad_mito.AN,
                max_hl=ht.gnomad_mito.max_hl,
            ),
            'helix': hl.Struct(
                ac=ht.helix_mito.AC_hom,
                af=ht.helix_mito.AF_hom,
                an=ht.helix_mito.AN,
            ),
            'helix_heteroplasmy': hl.Struct(
                ac=ht.helix_mito.AC_het,
                af=ht.helix_mito.AF_het,
                an=ht.helix_mito.AN,
                max_hl=ht.helix_mito.max_hl,
            ),
        },
        DatasetType.SV: lambda ht: {
            'gnomad_svs': hl.Struct(
                af=ht.gnomad_svs.AF,
                het=ht.gnomad_svs.N_HET,
                hom=ht.gnomad_svs.N_HOM,
                id=ht.gnomad_svs.ID,
            ),
        },
        DatasetType.GCNV: lambda ht: {
            'seqrPop': hl.Struct(
                ac=ht.gt_stats.AC,
                af=ht.gt_stats.AF,
                an=ht.gt_stats.AN,
                het=ht.gt_stats.Het,
                hom=ht.gt_stats.Hom,
            ),
        },
    }[dataset_type](ht)


def get_position_fields(ht: hl.Table, dataset_type: DatasetType):
    if dataset_type in {DatasetType.SV, DatasetType.GCNV}:
        rg37_contig = reference_independent_contig(ht.rg37_locus_end)
        return {
            'chrom': reference_independent_contig(ht.start_locus),
            'pos': ht.start_locus.position,
            'end': ht.end_locus.position,
            'rg37LocusEnd': hl.Struct(
                contig=rg37_contig,
                position=hl.or_missing(
                    hl.is_defined(rg37_contig),
                    ht.rg37_locus_end.position,
                ),
            ),
        }
    if dataset_type == DatasetType.MITO:
        return {
            'pos': ht.locus.position,
            'ref': ht.alleles[0],
            'alt': ht.alleles[1],
        }
    return {
        'chrom': reference_independent_contig(ht.locus),
        'pos': ht.locus.position,
        'ref': ht.alleles[0],
        'alt': ht.alleles[1],
    }


def get_variant_id_fields(
    ht: hl.Table,
    dataset_type: DatasetType,
):
    return {
        DatasetType.SNV_INDEL: lambda ht: {
            'variantId': ht.variant_id,
            'rsid': ht.rsid,
            'CAID': ht.CAID,
        },
        DatasetType.MITO: lambda ht: {
            'variantId': ht.variant_id,
            'rsid': ht.rsid,
        },
        DatasetType.SV: lambda ht: {
            'variantId': ht.variant_id,
        },
        DatasetType.GCNV: lambda ht: {
            'variantId': ht.variant_id,
        },
    }[dataset_type](ht)


def get_lifted_over_position_fields(ht: hl.Table, dataset_type: DatasetType):
    if dataset_type == DatasetType.MITO:
        return {'liftedOverPos': ht.rg37_locus.position}
    return {
        'liftedOverChrom': (
            reference_independent_contig(ht.rg37_locus)
            if hasattr(ht, 'rg37_locus')
            else reference_independent_contig(ht.rg38_locus)
        ),
        'liftedOverPos': (
            hl.or_missing(
                hl.is_defined(reference_independent_contig(ht.rg37_locus)),
                ht.rg37_locus.position,
            )
            if hasattr(ht, 'rg37_locus')
            else ht.rg38_locus.position
        ),
    }


def get_consequences_fields(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
):
    return {
        DatasetType.SNV_INDEL: lambda ht: {
            **(
                {
                    'sortedMotifFeatureConsequences': ht.sortedMotifFeatureConsequences,
                    'sortedRegulatoryFeatureConsequences': ht.sortedRegulatoryFeatureConsequences,
                }
                if reference_genome == ReferenceGenome.GRCh38
                else {}
            ),
            'sortedTranscriptConsequences': ht.sortedTranscriptConsequences,
        },
        DatasetType.MITO: lambda ht: {
            # MITO transcripts are not exported to their own table,
            # but the structure should be preserved here.
            'sortedTranscriptConsequences': hl.enumerate(
                ht.sortedTranscriptConsequences,
            ).starmap(reformat_transcripts_for_export),
        },
        DatasetType.SV: lambda ht: {
            'sortedGeneConsequences': ht.sortedGeneConsequences,
        },
        DatasetType.GCNV: lambda ht: {
            'sortedGeneConsequences': ht.sortedGeneConsequences,
        },
    }[dataset_type](ht)


def get_variants_export_fields(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
):
    return {
        'key_': ht.key_,
        'xpos': ht.xpos,
        **get_position_fields(ht, dataset_type),
        **get_variant_id_fields(ht, dataset_type),
        **get_lifted_over_position_fields(ht, dataset_type),
        **get_dataset_type_specific_annotations(ht, reference_genome, dataset_type),
        'predictions': hl.Struct(
            **get_predictions_export_fields(ht, reference_genome, dataset_type),
        ),
        'populations': hl.Struct(
            **get_populations_export_fields(ht, dataset_type),
        ),
        **get_consequences_fields(ht, reference_genome, dataset_type),
    }
