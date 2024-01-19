import hail as hl


def globalize_sample_ids(ht: hl.Table) -> hl.Table:
    row = ht.take(1)
    ht = ht.annotate_globals(
        sample_ids=(
            # NB: normal python expression here because the row is localized.
            # I had an easier time with this than hl.agg.take(1), which was an
            # alternative implementation.
            [e.s for e in row[0].entries] if (row and len(row[0].entries) > 0) else hl.empty_array(hl.tstr)
        ),
    )
    return ht.annotate(entries=ht.entries.map(lambda s: s.drop('s')))


def deglobalize_sample_ids(ht: hl.Table) -> hl.Table:
    ht = ht.annotate(
        entries=(
            hl.enumerate(ht.entries).starmap(
                lambda i, e: hl.Struct(**e, s=ht.sample_ids[i]),
            )
        ),
    )
    return ht.drop('sample_ids')


def filter_callset_entries(
    ht: hl.Table,
    sample_subset_ht: hl.Table,
) -> hl.Table:
    # Removes sample id calls that have been re-called.
    sample_ids = sample_subset_ht.aggregate(hl.agg.collect_as_set(sample_subset_ht.s))
    ht = deglobalize_sample_ids(ht)
    ht = ht.annotate(
        entries=(ht.entries.filter(lambda e: ~hl.set(sample_ids).contains(e.s))),
    )
    return globalize_sample_ids(ht)


def join_entries_hts(ht: hl.Table, callset_ht: hl.Table) -> hl.Table:
    ht = ht.join(callset_ht, 'outer')
    ht_empty_entries = ht.sample_ids.map(
        lambda _: hl.missing(ht.entries_1.dtype.element_type),
    )
    callset_ht_empty_entries = ht.sample_ids_1.map(
        lambda _: hl.missing(ht.entries_1.dtype.element_type),
    )
    ht = ht.select(
        filters=hl.or_else(ht.filters_1, ht.filters),
        entries=(
            hl.case()
            .when(hl.is_missing(ht.entries), ht_empty_entries.extend(ht.entries_1))
            .when(
                hl.is_missing(ht.entries_1),
                ht.entries.extend(callset_ht_empty_entries),
            )
            .default(ht.entries.extend(ht.entries_1))
        ),
    )
    return ht.transmute_globals(sample_ids=ht.sample_ids.extend(ht.sample_ids_1))
