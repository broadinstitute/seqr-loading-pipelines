import hail as hl

from v03_pipeline.lib.model import DatasetType


def compute_callset_family_entries_ht(
    dataset_type: DatasetType,
    mt: hl.MatrixTable,
    entries_fields: dict[str, hl.Expression],
) -> hl.Table:
    sample_id_to_family_guid = hl.dict(
        {
            s: family_guid
            for family_guid, sample_ids in hl.eval(mt.family_samples).items()
            for s in sample_ids
        },
    )
    ht = mt.select_rows(
        filters=mt.filters.difference(dataset_type.excluded_filters),
        family_entries=(
            hl.sorted(
                hl.agg.collect(
                    hl.Struct(
                        s=mt.s,
                        family_guid=sample_id_to_family_guid[mt.s],
                        **entries_fields,
                    ),
                )
                .group_by(lambda e: e.family_guid)
                .values()
                .map(
                    lambda fe: hl.sorted(fe, key=lambda e: e.s),
                ),
                lambda fe: fe[0].family_guid,
            )
        ),
    ).rows()
    # NB: globalize before we send families to missing or filter rows
    ht = globalize_ids(ht)
    ht = ht.annotate(
        family_entries=(
            ht.family_entries.map(
                lambda fe: hl.or_missing(
                    fe.any(dataset_type.family_entries_filter_fn),
                    fe,
                ),
            )
        ),
    )
    # Only keep rows where at least one family is not missing.
    return ht.filter(ht.family_entries.any(lambda fe: ~hl.is_missing(fe)))


def globalize_ids(ht: hl.Table) -> hl.Table:
    row = ht.take(1)
    ht = ht.annotate_globals(
        family_guids=(
            [fe[0].family_guid for fe in row[0].family_entries]
            if (row and len(row[0].family_entries) > 0)
            else hl.empty_array(hl.tstr)
        ),
        family_samples=(
            {fe[0].family_guid: [e.s for e in fe] for fe in row[0].family_entries}
            if (row and len(row[0].family_entries) > 0)
            else hl.empty_dict(hl.tstr, hl.tarray(hl.tstr))
        ),
    )
    return ht.annotate(
        family_entries=ht.family_entries.map(
            lambda fe: fe.map(lambda se: se.drop('s', 'family_guid')),
        ),
    )


def deglobalize_ids(ht: hl.Table) -> hl.Table:
    ht = ht.annotate(
        family_entries=(
            hl.enumerate(ht.family_entries).starmap(
                lambda i, fe: hl.enumerate(fe).starmap(
                    lambda j, e: hl.Struct(
                        **e,
                        s=ht.family_samples[ht.family_guids[i]][j],
                        family_guid=ht.family_guids[i],
                    ),
                ),
            )
        ),
    )
    return ht.drop('family_guids', 'family_samples')


def splice_new_callset_family_guids(
    ht: hl.Table,
    family_guids: list[str],
) -> hl.Table:
    # Remove families from the existing project table structure (both the entries arrays and the globals are mutated)
    family_indexes_to_keep = [
        i
        for i, f in enumerate(hl.eval(ht.globals.family_guids))
        if f not in family_guids
    ]
    ht = ht.annotate(
        family_entries=(
            hl.array(family_indexes_to_keep).map(lambda i: ht.family_entries[i])
            if len(family_indexes_to_keep) > 0
            else hl.missing(ht.family_entries.dtype.element_type)
        ),
    )
    return ht.annotate_globals(
        family_guids=ht.family_guids.filter(
            lambda f: ~hl.set(family_guids).contains(f),
        ),
        family_samples=hl.dict(
            ht.family_samples.items().filter(
                lambda i: ~hl.set(family_guids).contains(i[0]),
            ),
        ),
    )


def join_family_entries_hts(ht: hl.Table, callset_ht: hl.Table) -> hl.Table:
    ht = ht.join(callset_ht, 'outer')
    ht_empty_family_entries = ht.family_guids.map(
        lambda _: hl.missing(ht.family_entries_1.dtype.element_type),
    )
    callset_ht_empty_family_entries = ht.family_guids_1.map(
        lambda _: hl.missing(ht.family_entries_1.dtype.element_type),
    )
    ht = ht.select(
        filters=hl.or_else(ht.filters_1, ht.filters),
        family_entries=(
            hl.case()
            .when(
                hl.is_missing(ht.family_entries),
                ht_empty_family_entries.extend(ht.family_entries_1),
            )
            .when(
                hl.is_missing(ht.family_entries_1),
                ht.family_entries.extend(callset_ht_empty_family_entries),
            )
            .default(ht.family_entries.extend(ht.family_entries_1))
        ),
    )
    # NB: transume because we want to drop the *_1 fields, but preserve other globals
    return ht.transmute_globals(
        family_guids=ht.family_guids.extend(ht.family_guids_1),
        family_samples=hl.dict(
            ht.family_samples.items().extend(ht.family_samples_1.items()),
        ),
    )
