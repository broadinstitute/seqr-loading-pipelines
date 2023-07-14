import hail as hl


def globalize_sample_ids(ht: hl.Table) -> hl.Table:
    ht = ht.annotate_globals(
        sample_ids=ht.aggregate(hl.agg.take(ht.entries.sample_id, 1)[0]),
    )
    return ht.annotate(entries=ht.entries.map(lambda s: s.drop('sample_id')))


def deglobalize_sample_ids(ht: hl.Table) -> hl.Table:
    ht = ht.annotate(
        entries=(
            hl.zip_with_index(ht.entries).starmap(
                lambda i, e: hl.Struct(**e, sample_id=ht.sample_ids[i]),
            )
        ),
    )
    return ht.drop('sample_ids')

def remove_callset_sample_ids(
    sample_entries_ht: hl.Table,
    sample_subset_ht: hl.Table,
) -> hl.Table:
    sample_ids = sample_subset_ht.aggregate(hl.agg.collect_as_set(sample_subset_ht.s))
    return sample_entries_ht.annotate(
        entries=(
            sample_entries_ht.entries.filter(lambda e: ~hl.set(sample_ids).contains(e.sample_id))
        ),
    )

def union_entries_hts(sample_entries_ht: hl.Table, callset_ht: hl.Table) -> hl.Table:
    sample_entries_ht = sample_entries_ht.join(callset_ht, 'outer')
    ht_empty_entries = sample_entries_ht.sample_ids.map(lambda x: hl.missing(sample_entries_ht.entries.dtype.element_type))
    callset_ht_empty_entries = sample_entries_ht.sample_ids_1.map(lambda x: hl.missing(sample_entries_ht.entries_1.dtype.element_type))
    return sample_entries_ht.select(
        filters=hl.or_else(sample_entries_ht.filters_1, sample_entries_ht.filters),
        entries=(
            hl.case()
            .when(hl.is_missing(sample_entries_ht.entries), ht_empty_entries.extend(sample_entries_ht.entries_1))
            .when(
                hl.is_missing(sample_entries_ht.entries_1),
                sample_entries_ht.entries.extend(callset_ht_empty_entries),
            )
            .default(sample_entries_ht.entries.extend(sample_entries_ht.entries_1))
        ),
    )
