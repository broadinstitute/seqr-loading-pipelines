import hail as hl

from v03_pipeline.lib.model import DatasetType


def compute_callset_family_entries_ht(dataset_type: DatasetType, mt: hl.MatrixTable, entries_fields: dict[str, hl.Expression]) -> hl.Table:
    sample_id_to_family_guid = hl.dict({
        s: f.family_guid
        for f in mt.families
        for s in f.samples
    })
    callset_ht = mt.select_rows(
        filters=mt.filters.difference(dataset_type.excluded_filters),
        family_entries=(
            hl.agg.collect(
                hl.Struct(
                    s=mt.s,
                    family_guid=sample_id_to_family_guid[mt.s]
                    **entries_fields,
                ),
            )
            .group_by(lambda e: e.family_guid)
            .values()
            .map(
                # In English:
                # For each grouped family, if any of the samples are "non-ref"
                # keep the family and sort by the sample id.  If all of the
                # samples are "ref", replace the family with missing.
                lambda fe: hl.or_missing(
                    fe.any(dataset_type.sample_entries_filter_fn),
                    hl.sorted(fe, key=lambda e: e.s),
                ),
            )
        ),
    ).rows()
    # Filter out rows where
    callset_ht = callset_ht.filter(
        callset_ht.family_entries.any(lambda fe: ~hl.is_missing(fe)),
    )
    return globalize_ids(callset_ht)

def globalize_ids(ht: hl.Table) -> hl.Table:
    row = ht.take(1)
    ht = ht.annotate_globals(
        family_guids=(
            [fe[0].f for fe in row[0].family_entries]
            if (row and len(row[0].family_entries) > 0)
            else hl.empty_array(hl.tstr)
        ),
        family_samples=(
            {
                fe[0].f: [e.s for e in fe] for fe in row[0].family_entries
            }
            if (row and len(row[0].family_entries) > 0)
            else hl.empty_dict(hl.tstr, hl.tarray(hl.tstr))
        ),
    )
    return ht.annotate(family_entries=ht.family_entries.map(lambda s: s.drop('s', 'family_guid')))


def deglobalize_ids(ht: hl.Table) -> hl.Table:
    ht = ht.annotate(
        family_entries=(
            hl.enumerate(ht.family_entries).starmap(
                lambda i, fe: fe.starmap(
                    lambda j, e: hl.Struct(**e, s=ht.family_samples[ht.family_guids[i]][j], family_guid=ht.family_guids[i]),
                ),
            )
        ),
    )
    return ht.drop('family_guids', 'family_samples')


def filter_new_callset_family_guids(
    ht: hl.Table,
    family_guids: list[str],
) -> hl.Table:
    family_indexes_to_keep = [i for i, f in enumerate(hl.eval(ht.globals.family_guids)) if f not in family_guids]
    ht = ht.annotate(
        family_entries=family_indexes_to_keep.map(lambda i: ht.family_entries[i]),
    )
    return ht.annotate_globals(
        family_guids=ht.family_guids.filter(lambda f: ~hl.set(family_guids).contains(f)),
        family_samples=hl.dict(ht.family_guids.items().filter(lambda f, _: ~hl.set(family_guids).contains(f))),
    )


def join_family_entries_hts(ht: hl.Table, callset_ht: hl.Table) -> hl.Table:
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
