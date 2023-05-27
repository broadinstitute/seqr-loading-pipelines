import hail as hl

from v03_pipeline.lib.model import Env, ReferenceGenome


def hgmd(
    mt: hl.MatrixTable,
    env: Env,
    reference_genome: ReferenceGenome,
) -> hl.MatrixTable:
    reference_dataset_collection_ht = hl.read_table(
        reference_dataset_collection_path(
            env,
            reference_genome,
            reference_dataset_collection,
        ),
    )
    return reference_dataset_collection_ht[mt.row_key].hgmd


def gnomad_non_coding_constraint(
    mt: hl.MatrixTable,
    env: Env,
    reference_genome: ReferenceGenome,
) -> hl.MatrixTable:
    return hl.Struct(
        z_score=(
            reference_dataset_collection_ht.index(mt.locus, all_matches=True)
            .filter(
                lambda x: hl.is_defined(x.gnomad_non_coding_constraint['z_score']),
            )
            .gnomad_non_coding_constraint.z_score.first()
        ),
    )


def screen(
    mt: hl.MatrixTable,
    env: Env,
    reference_genome: ReferenceGenome,
) -> hl.MatrixTable:
    return hl.Struct(
        region_type_id=(
            reference_dataset_collection_ht.index(
                ht.locus,
                all_matches=True,
            ).flatmap(
                lambda x: x.screen['region_type_id'],
            )
        ),
    )
