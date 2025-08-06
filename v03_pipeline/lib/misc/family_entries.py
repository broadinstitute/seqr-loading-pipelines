import hail as hl

from v03_pipeline.lib.model import DatasetType, ReferenceGenome


def initialize_project_table(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
):
    key_type = dataset_type.table_key_type(reference_genome)
    return hl.Table.parallelize(
        [],
        hl.tstruct(
            **key_type,
            filters=hl.tset(hl.tstr),
            # NB: entries is missing here because it is untyped
            # until we read the type off of the first callset aggregation.
        ),
        key=key_type.fields,
        globals=hl.Struct(
            family_guids=hl.empty_array(hl.tstr),
            family_samples=hl.empty_dict(hl.tstr, hl.tarray(hl.tstr)),
            updates=hl.empty_set(
                hl.tstruct(callset=hl.tstr, remap_pedigree_hash=hl.tint32),
            ),
        ),
    )


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
            # NB: we're sorted by both family and sample when this runs.
            # However, the sort is not guaranteed once the entries
            # table is edited and families are spliced out and re-appended.
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
    # NB: globalize before we set families to missing
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
    return ht.filter(ht.family_entries.any(hl.is_defined))


def globalize_ids(ht: hl.Table) -> hl.Table:
    row = ht.take(1)[0] if ht.count() > 0 else None
    has_family_entries = row and len(row.family_entries) > 0
    ht = ht.annotate_globals(
        family_guids=(
            [fe[0].family_guid for fe in row.family_entries]
            if has_family_entries
            else hl.empty_array(hl.tstr)
        ),
        family_samples=(
            {fe[0].family_guid: [e.s for e in fe] for fe in row.family_entries}
            if has_family_entries
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


def remove_family_guids(
    ht: hl.Table,
    family_guids: hl.SetExpression,
) -> hl.Table:
    # Remove families from the existing project table structure (both the entries arrays and the globals are mutated)
    family_indexes_to_keep = hl.eval(
        hl.array(
            hl.enumerate(ht.globals.family_guids)
            .filter(lambda item: ~family_guids.contains(item[1]))
            .map(lambda item: item[0]),
        ),
    )
    ht = ht.annotate(
        # NB: this "should" work without the extra if statement (and does in the tests)
        # however, experiments on dataproc showed this statement hanging with an empty
        # unevaluated indexes array.
        family_entries=hl.array(family_indexes_to_keep).map(
            lambda i: ht.family_entries[i],
        )
        if len(family_indexes_to_keep) > 0
        else hl.empty_array(ht.family_entries.dtype.element_type),
    )
    ht = ht.filter(hl.any(ht.family_entries.map(hl.is_defined)))
    return ht.annotate_globals(
        family_guids=ht.family_guids.filter(
            lambda f: ~family_guids.contains(f),
        ),
        family_samples=hl.dict(
            ht.family_samples.items().filter(
                lambda item: ~family_guids.contains(item[0]),
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
    # NB: transmute because we want to drop the *_1 fields, but preserve other globals
    return ht.transmute_globals(
        family_guids=ht.family_guids.extend(ht.family_guids_1),
        family_samples=hl.dict(
            ht.family_samples.items().extend(ht.family_samples_1.items()),
        ),
    )


def deduplicate_by_most_non_ref_calls(ht: hl.Table) -> hl.Table:
    ht = ht.annotate(
        non_ref_count=hl.len(
            hl.flatten(ht.family_entries).filter(lambda s: s.GT.is_non_ref()),
        ),
    )
    return ht.group_by(*ht.key).aggregate(
        filters=hl.agg.take(ht.filters, 1, ordering=-ht.non_ref_count)[0],
        family_entries=hl.agg.take(ht.family_entries, 1, ordering=-ht.non_ref_count)[0],
    )
