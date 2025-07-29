import itertools
import math

import hail as hl

from v03_pipeline.lib.annotations import sv
from v03_pipeline.lib.misc.pedigree import Family
from v03_pipeline.lib.model import ReferenceGenome, Sex

WRONG_CHROM_PENALTY = 1e9


def _get_grouped_new_callset_variants(
    mt: hl.MatrixTable,
    duplicate_internal_variant_ids: set[str],
) -> itertools.groupby:
    mt = mt.select_rows(
        'info.SEQR_INTERNAL_TRUTH_VID',
        end_locus=sv.end_locus(mt),
    )
    return itertools.groupby(
        sorted(
            mt.filter_rows(
                duplicate_internal_variant_ids.contains(
                    mt['info.SEQR_INTERNAL_TRUTH_VID'],
                ),
            )
            .rows()
            .collect(),
            key=lambda x: x['info.SEQR_INTERNAL_TRUTH_VID'],
        ),
        lambda x: x['info.SEQR_INTERNAL_TRUTH_VID'],
    )


def deduplicate_merged_sv_concordance_calls(
    mt: hl.MatrixTable,
    annotations_ht: hl.Table,
) -> hl.MatrixTable:
    # First find the seqr internal variant ids that are duplicated in the new callset.
    duplicate_internal_variant_ids = hl.set(
        {
            k
            for k, v in mt.aggregate_rows(
                hl.agg.counter(mt['info.SEQR_INTERNAL_TRUTH_VID']),
            ).items()
            if v > 1
        }
        or hl.empty_set(hl.tstr),
    )

    # Then, collect into memory the necessary existing variants & the new variants
    annotations_ht = annotations_ht.select('end_locus')
    existing_variants = {
        v.variant_id: v
        for v in (
            annotations_ht.filter(
                duplicate_internal_variant_ids.contains(annotations_ht.variant_id),
            ).collect()
        )
    }
    grouped_new_variants = _get_grouped_new_callset_variants(
        mt,
        duplicate_internal_variant_ids,
    )

    # Then, iterate over new variants and exclude all but the best match
    new_variant_ids_to_exclude = set()
    for existing_variant_id, new_variants in grouped_new_variants:
        existing_variant = existing_variants[existing_variant_id]
        closest_variant_id, min_distance = None, math.inf

        # First pass to find the closest variant
        new_variants_it1, new_variants_it2 = itertools.tee(new_variants, 2)
        for new_variant in new_variants_it1:
            distance = math.fabs(
                new_variant.end_locus.position
                - existing_variant.end_locus.position
                + (
                    WRONG_CHROM_PENALTY
                    if (
                        new_variant.end_locus.contig
                        != existing_variant.end_locus.contig
                    )
                    else 0
                ),
            )
            if distance < min_distance:
                min_distance = distance
                closest_variant_id = new_variant.variant_id

        # Second pass to exclude all but the closest.
        for new_variant in new_variants_it2:
            if new_variant.variant_id != closest_variant_id:
                new_variant_ids_to_exclude.add(new_variant.variant_id)

    # Finally, remove SEQR_INTERNAL_TRUTH_VID from those variants.
    return mt.annotate_rows(
        **{
            'info.SEQR_INTERNAL_TRUTH_VID': hl.if_else(
                hl.set(new_variant_ids_to_exclude or hl.empty_set(hl.tstr)).contains(
                    mt.variant_id,
                ),
                hl.missing(hl.tstr),
                mt['info.SEQR_INTERNAL_TRUTH_VID'],
            ),
        },
    )
