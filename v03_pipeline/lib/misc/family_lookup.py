import hail as hl

from v03_pipeline.lib.model import DatasetType


def compute_callset_family_lookup_ht(
    dataset_type: DatasetType,
    mt: hl.MatrixTable,
    project_guid: str,
) -> hl.Table:
    sample_id_to_family_guid = hl.dict(
        {
            s: family_guid
            for family_guid, sample_ids in hl.eval(mt.family_samples).items()
            for s in sample_ids
        },
    )
    ht = mt.select_rows(
        project_stats=hl.array([
            hl.agg.collect(
                hl.Struct(
                    family_guid=sample_id_to_family_guid[mt.s],
                    **mt.entry,
                ),
            )
            .group_by(lambda fs: fs.family_guid)
            .values()
            .map(
                lambda fs: hl.Struct(
                    family_guid=fs[0].family_guid,
                    **{
                        field_name: hl.len(fs.filter(filter_fn))
                        for field_name, filter_fn in dataset_type.family_lookup_table_fields_and_genotype_filter_fns.items()
                    },
                ),
            ),
        ]),
    ).rows()
    return globalize_ids(ht, project_guid)


def globalize_ids(ht: hl.Table, project_guid: str) -> hl.Table:
    row = ht.take(1)
    has_project_stats = row and len(row[0].project_stats) > 0
    ht = ht.annotate_globals(
        project_guids=[project_guid],
        project_families=(
            {project_guid: [fs.family_guid for fs in ps] for ps in row[0].project_stats}
            if has_project_stats
            else hl.empty_dict(hl.tstr, hl.tarray(hl.tstr))
        ),
    )
    return ht.annotate(
        project_stats=ht.project_stats.map(
            lambda ps: ps.map(lambda fs: fs.drop('family_guid')),
        ),
    )


def remove_new_callset_family_guids(
    ht: hl.Table,
    project_guid: str,
    family_guids: list[str],
) -> hl.Table:
    if project_guid not in hl.eval(ht.globals.project_families):
        return ht
    family_guids = hl.set(family_guids)
    project_i = ht.project_guids.index(project_guid)
    family_indexes_to_keep = hl.array(
        hl.enumerate(ht.globals.project_families[project_guid])
        .filter(lambda item: ~family_guids.contains(item[1]))
        .map(lambda item: item[0]),
    )
    ht = ht.annotate(
        project_stats=(
            hl.enumerate(ht.project_stats).starmap(
                lambda i, fs: (
                    hl.if_else(
                        i != project_i,
                        fs,
                        family_indexes_to_keep.map(
                            lambda j: ht.project_stats[i][j],
                        ),
                    )
                ),
            )
        ),
    )
    return ht.annotate_globals(
        project_families=hl.dict(
            ht.project_families.items().map(
                lambda item: (
                    hl.if_else(
                        item[0] != project_guid,
                        item,
                        (item[0], ht.project_families[project_guid].filter(
                            lambda family_guid: ~family_guids.contains(family_guid),
                        )),
                    )
                ),
            ),
        ),
    )


def join_family_lookup_hts(
    ht: hl.Table,
    callset_ht: hl.Table,
    project_guid: str,
) -> hl.Table:
    project_i = ht.project_guids.index(project_guid)
    ht = ht.join(callset_ht, 'outer')
    ht_empty_project_stats = ht.project_guids.map(
        lambda _: hl.missing(ht.project_stats.dtype.element_type),
    )
    callset_ht_empty_project_stats = ht.project_guids_1.map(
        lambda _: hl.missing(ht.project_stats_1.dtype.element_type),
    )
    ht = ht.select(
        # We have 4 unique cases here.
        # 1) The row is missing on the left but preset on the right.
        #   - We populate the left with a missing value for each project
        #     on the left and extend on the right.
        # 2) The row is present on the right but missing on the left.
        #   - We populate the right with a missing value for each project
        #     on the right (there should only be one), and extend with
        #     that missing value on the right.
        # 3) The row is present on both the right and the left, and the
        #    project has never been seen before on the left.
        #   - We just extend the array from the left with the project from the right.
        # 4) The row is present on both the righ and the left, and the project
        #    has been loaded before (there may be either no families present or some that
        #    are not present in this callset).
        #   - We keep all entries in project_stats the same "except" at the project index
        #     and add the new families to that project.
        project_stats=(
            hl.case()
            .when(
                hl.is_missing(ht.project_stats),
                ht_empty_project_stats.extend(ht.project_stats),
            )
            .when(
                hl.is_missing(ht.project_stats_1),
                ht.project_stats.extend(callset_ht_empty_project_stats),
            )
            .default(
                hl.if_else(
                    hl.is_missing(project_i),
                    ht.project_stats.extend(ht.project_stats_1),
                    hl.enumerate(ht.project_stats).starmap(
                        lambda i, fs: (
                            hl.if_else(
                                i != project_i,
                                fs,
                                ht.project_stats[project_i].extend(ht.project_stats[0]),
                            )
                        ),
                    ),
                ),
            )
        ),
    )
    return ht.transmute_globals(
        project_guids=hl.if_else(
            hl.is_missing(project_i),
            ht.project_guids.extend(ht.project_guids_1),
            ht.project_guids,
        ),
        project_families=hl.if_else(
            hl.is_missing(project_i),
            hl.dict(ht.project_families.items().extend(ht.project_families_1.items())),
            hl.dict(
                ht.project_families.items().map(
                    lambda item: hl.if_else(
                        item[0] != project_guid,
                        item,
                        (item[0], ht.project_families[project_guid].extend(ht.project_families_1[project_guid])),
                    ),
                ),
            ),
        ),
    )
