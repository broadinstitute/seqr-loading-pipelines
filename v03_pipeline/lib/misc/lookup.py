import hail as hl

from v03_pipeline.lib.model import DatasetType, SampleType


def compute_callset_lookup_ht(
    dataset_type: DatasetType,
    mt: hl.MatrixTable,
    project_guid: str,
    sample_type: SampleType,
) -> hl.Table:
    sample_id_to_family_guid = hl.dict(
        {
            s: family_guid
            for family_guid, sample_ids in hl.eval(mt.family_samples).items()
            for s in sample_ids
        },
    )
    ht = mt.select_rows(
        project_stats=hl.array(
            [
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
                            for field_name, filter_fn in dataset_type.lookup_table_fields_and_genotype_filter_fns.items()
                        },
                    ),
                ),
            ],
        ),
    ).rows()
    ht = globalize_ids(ht, project_guid, sample_type)
    return ht.annotate(
        project_stats=[
            # Set a family to missing if all values are 0
            ht.project_stats[0].map(
                lambda ps: hl.or_missing(
                    hl.sum(list(ps.values())) > 0,
                    ps,
                ),
            ),
        ],
    )


def globalize_ids(ht: hl.Table, project_guid: str, sample_type: SampleType) -> hl.Table:
    row = ht.take(1)[0] if ht.count() > 0 else None
    has_project_stats = row and len(row.project_stats) > 0
    project_key = (project_guid, sample_type.value)
    ht = ht.annotate_globals(
        project_sample_types=[project_key],
        project_families=(
            {project_key: [fs.family_guid for fs in ps] for ps in row.project_stats}  # noqa: B035
            if has_project_stats
            else hl.empty_dict(hl.tstr, hl.tarray(hl.tstr))
        ),
    )
    return ht.annotate(
        project_stats=ht.project_stats.map(
            lambda ps: ps.map(lambda fs: fs.drop('family_guid')),
        ),
    )


def remove_family_guids(
    ht: hl.Table,
    project_guid: str,
    sample_type: SampleType,
    family_guids: hl.SetExpression,
) -> hl.Table:
    project_key = (project_guid, sample_type.value)
    if project_key not in hl.eval(ht.globals.project_families):
        return ht
    project_i = ht.project_sample_types.index(project_key)
    family_indexes_to_keep = hl.eval(
        hl.array(
            hl.enumerate(ht.globals.project_families[project_key])
            .filter(lambda item: ~family_guids.contains(item[1]))
            .map(lambda item: item[0]),
        ),
    )
    ht = ht.annotate(
        project_stats=(
            hl.enumerate(ht.project_stats).starmap(
                lambda i, fs: (
                    hl.if_else(
                        i != project_i,
                        fs,
                        (
                            hl.array(family_indexes_to_keep).map(
                                lambda j: ht.project_stats[i][j],
                            )
                            if len(family_indexes_to_keep) > 0
                            else hl.empty_array(ht.project_stats[i].dtype.element_type)
                        ),
                    )
                ),
            )
        ),
    )
    ht = ht.filter(
        hl.any(ht.project_stats.map(lambda fs: hl.any(fs.map(hl.is_defined)))),
    )
    return ht.annotate_globals(
        project_families=hl.dict(
            ht.project_families.items().map(
                lambda item: (
                    hl.if_else(
                        item[0] != project_key,
                        item,
                        (
                            item[0],
                            ht.project_families[project_key].filter(
                                lambda family_guid: ~family_guids.contains(family_guid),
                            ),
                        ),
                    )
                ),
            ),
        ),
    )


def remove_project(
    ht: hl.Table,
    project_guid: str,
    sample_type: SampleType,
) -> hl.Table:
    existing_projects = hl.eval(ht.globals.project_sample_types)
    project_key = (project_guid, sample_type.value)
    if project_key not in existing_projects:
        return ht
    project_indexes_to_keep = hl.eval(
        hl.enumerate(existing_projects)
        .filter(lambda item: item[1] != project_key)
        .map(lambda item: item[0]),
    )
    ht = ht.annotate(
        project_stats=(
            # See "remove_family_guids" func for why this was necessary
            hl.array(project_indexes_to_keep).map(lambda i: ht.project_stats[i])
            if len(project_indexes_to_keep) > 0
            else hl.empty_array(ht.project_stats.dtype.element_type)
        ),
    )
    ht = ht.filter(hl.any(ht.project_stats.map(hl.is_defined)))
    return ht.annotate_globals(
        project_sample_types=ht.project_sample_types.filter(
            lambda p: p != project_key,
        ),
        project_families=hl.dict(
            ht.project_families.items().filter(lambda item: item[0] != project_key),
        ),
    )


def join_lookup_hts(
    ht: hl.Table,
    callset_ht: hl.Table,
) -> hl.Table:
    ht = ht.join(callset_ht, 'outer')
    project_key = ht.project_sample_types_1[0]
    ht_project_i = ht.project_sample_types.index(project_key)
    ht = ht.select(
        # We have 6 unique cases here.
        # 1) The project has not been loaded before, the row is missing
        #    on the left but present on the right.
        # 2) The project has not been loaded before, the row is present
        #    on the left but missing on the right.
        # 3) The project as not been loaded before, the row is present on
        #    both the left and right.
        # 4) The project has been loaded before, the row is missing on the
        #    left but present on the right.
        # 5) The project has been loaded before, the row is present on the
        #    left but missing on the right.
        # 6) The project has been loaded before, the row is present on both
        #    the left and right.
        project_stats=(
            hl.case()
            .when(
                (hl.is_missing(ht_project_i) & hl.is_missing(ht.project_stats)),
                ht.project_sample_types.map(
                    lambda _: hl.missing(ht.project_stats.dtype.element_type),
                ).extend(ht.project_stats_1),
            )
            .when(
                (hl.is_missing(ht_project_i) & hl.is_missing(ht.project_stats_1)),
                ht.project_stats.extend(
                    ht.project_sample_types_1.map(
                        lambda _: hl.missing(ht.project_stats_1.dtype.element_type),
                    ),
                ),
            )
            .when(
                hl.is_missing(ht_project_i),
                ht.project_stats.extend(ht.project_stats_1),
            )
            .when(
                hl.is_missing(ht.project_stats),
                hl.enumerate(ht.project_sample_types).starmap(
                    # Add a missing project_stats value for every loaded project,
                    # then add a missing value for every family for "this project"
                    # and extend the new families on the right.
                    lambda i, p: (
                        hl.or_missing(
                            i == ht_project_i,
                            ht.project_families[p]
                            .map(
                                lambda _: hl.missing(
                                    ht.project_stats.dtype.element_type.element_type,
                                ),
                            )
                            .extend(ht.project_stats_1[0]),
                        )
                    ),
                ),
            )
            .when(
                hl.is_missing(ht.project_stats_1),
                hl.enumerate(ht.project_stats).starmap(
                    # At the specific index of "this project"
                    # extend with missing values on the right for every
                    # newly loaded family.
                    lambda i, ps: (
                        hl.if_else(
                            i != ht_project_i,
                            ps,
                            ps.extend(
                                ht.project_families_1[project_key].map(
                                    lambda _: hl.missing(
                                        ht.project_stats.dtype.element_type.element_type,
                                    ),
                                ),
                            ),
                        )
                    ),
                ),
            )
            .default(
                hl.enumerate(ht.project_stats).starmap(
                    lambda i, ps: (
                        hl.if_else(
                            i != ht_project_i,
                            ps,
                            ht.project_stats[ht_project_i].extend(
                                ht.project_stats_1[0],
                            ),
                        )
                    ),
                ),
            )
        ),
    )
    # NB: double reference these because the source ht has changed :/
    project_key = ht.project_sample_types_1[0]
    ht_project_i = ht.project_sample_types.index(project_key)
    return ht.transmute_globals(
        project_sample_types=hl.if_else(
            hl.is_missing(ht_project_i),
            ht.project_sample_types.extend(ht.project_sample_types_1),
            ht.project_sample_types,
        ),
        project_families=hl.if_else(
            hl.is_missing(ht_project_i),
            hl.dict(ht.project_families.items().extend(ht.project_families_1.items())),
            hl.dict(
                ht.project_families.items().map(
                    lambda item: hl.if_else(
                        item[0] != project_key,
                        item,
                        (
                            item[0],
                            ht.project_families[project_key].extend(
                                ht.project_families_1[project_key],
                            ),
                        ),
                    ),
                ),
            ),
        ),
    )
