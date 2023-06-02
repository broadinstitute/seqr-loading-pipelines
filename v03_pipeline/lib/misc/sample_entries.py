import hail as hl


def _empty_entries(ht: hl.Table) -> hl.StructExpression:
    first_entries_row = ht.aggregate(hl.agg.take(ht.entries, 1))
    if not len(first_entries_row):
        return hl.empty_array(ht.entries.dtype.element_type)
    return [
        hl.Struct(
            **{
                k: hl.missing(v)
                for k, v in ht.entries.dtype.element_type.items()
                if k != 'sample_id'
            },
            sample_id=e.sample_id,
        )
        for e in first_entries_row[0]
    ]


def globalize_sample_ids(ht: hl.Table) -> hl.Table:
    first_entries_row = ht.aggregate(hl.agg.take(ht.entries, 1))
    ht = ht.annotate_globals(
        sample_ids=[e.sample_id for e in first_entries_row[0]],
    )
    return ht.select(entries=ht.entries.map(lambda s: s.drop('sample_id')))


def deglobalize_sample_ids(ht: hl.Table) -> hl.Table:
    ht = ht.select(
        entries=(
            hl.zip_with_index(ht.entries).starmap(
                lambda i, e: hl.Struct(**e, sample_id=ht.sample_ids[i]),
            )
        ),
    )
    return ht.drop('sample_ids')


def union_entries_hts(ht: hl.Table, callset_ht: hl.Table) -> hl.Table:
    ht_empty_entries = _empty_entries(ht)
    callset_ht_empty_entries = _empty_entries(callset_ht)
    ht = ht.join(callset_ht, 'outer')
    return ht.select(
        entries=hl.sorted(
            (
                hl.case()
                .when(hl.is_missing(ht.entries), ht.entries_1.extend(ht_empty_entries))
                .when(
                    hl.is_missing(ht.entries_1),
                    ht.entries.extend(callset_ht_empty_entries),
                )
                .default(ht.entries.extend(ht.entries_1))
            ),
            key=lambda e: e.sample_id,
        ),
    )
