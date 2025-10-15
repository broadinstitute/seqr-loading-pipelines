import hail as hl

from v03_pipeline.lib.annotations.enums import (
    BIOTYPES,
    FIVEUTR_CONSEQUENCES,
    LOF_FILTERS,
    MITOTIP_PATHOGENICITIES,
    MOTIF_CONSEQUENCE_TERMS,
    REGULATORY_BIOTYPES,
    REGULATORY_CONSEQUENCE_TERMS,
    SV_CONSEQUENCE_RANKS,
    SV_TYPE_DETAILS,
    SV_TYPES,
    TRANSCRIPT_CONSEQUENCE_TERMS,
)
from v03_pipeline.lib.core import DatasetType
from v03_pipeline.lib.core.definitions import ReferenceGenome
from v03_pipeline.lib.paths import valid_reference_dataset_path
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset


def annotate_reference_dataset_globals(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> hl.Table:
    for (
        reference_dataset
    ) in ReferenceDataset.for_reference_genome_dataset_type_annotations(
        reference_genome,
        dataset_type,
    ):
        rd_ht = hl.read_table(
            valid_reference_dataset_path(reference_genome, reference_dataset),
        )
        rd_ht_globals = rd_ht.index_globals()
        ht = ht.annotate_globals(
            versions=hl.Struct(
                **ht.globals.versions,
                **{reference_dataset.name: rd_ht_globals.version},
            ),
            enums=hl.Struct(
                **ht.globals.enums,
                **{reference_dataset.name: rd_ht_globals.enums},
            ),
        )
    return ht


def annotate_formatting_annotation_enum_globals(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> hl.Table:
    formatting_annotation_names = {
        fa.__name__ for fa in dataset_type.formatting_annotation_fns(reference_genome)
    }
    if 'sorted_motif_feature_consequences' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(
                sorted_motif_feature_consequences=hl.Struct(
                    consequence_term=MOTIF_CONSEQUENCE_TERMS,
                ),
            ),
        )
    if 'sorted_regulatory_feature_consequences' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(
                sorted_regulatory_feature_consequences=hl.Struct(
                    biotype=REGULATORY_BIOTYPES,
                    consequence_term=REGULATORY_CONSEQUENCE_TERMS,
                ),
            ),
        )
    if 'sorted_transcript_consequences' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(
                sorted_transcript_consequences=hl.Struct(
                    biotype=BIOTYPES,
                    consequence_term=TRANSCRIPT_CONSEQUENCE_TERMS,
                    **(
                        {
                            'loftee': hl.Struct(
                                lof_filter=LOF_FILTERS,
                            ),
                            'utrannotator': hl.Struct(
                                fiveutr_consequence=FIVEUTR_CONSEQUENCES,
                            ),
                        }
                        if reference_genome == ReferenceGenome.GRCh38
                        and dataset_type == DatasetType.SNV_INDEL
                        else {
                            'lof_filter': LOF_FILTERS,
                        }
                    ),
                ),
            ),
        )
    if 'mitotip' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(
                mitotip=hl.Struct(
                    trna_prediction=MITOTIP_PATHOGENICITIES,
                ),
            ),
        )
    if 'sv_type_id' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(
                sv_type=SV_TYPES,
            ),
        )
    if 'sv_type_detail_id' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(sv_type_detail=SV_TYPE_DETAILS),
        )
    if 'sorted_gene_consequences' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(
                sorted_gene_consequences=hl.Struct(
                    major_consequence=SV_CONSEQUENCE_RANKS,
                ),
            ),
        )
    return ht
