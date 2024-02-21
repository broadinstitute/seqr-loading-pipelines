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
        family_stats=hl.agg.collect(
            hl.Struct(
                family_guid=sample_id_to_family_guid[mt.s],
                **mt.entry,
            )
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
            )
        )
    ).rows()
    return globalize_ids(ht, project_guid)


def globalize_ids(ht: hl.Table, project_guid: str) -> hl.Table:
    row = ht.take(1)
    has_family_stats = row and len(row[0].family_stats) > 0
    ht = ht.annotate_globals(
        project_guids=[project_guid],
        project_families=(
            {project_guid: [fs.family_guid for fs in row[0].family_stats]}
            if has_family_stats
            else hl.empty_dict(hl.tstr, hl.tarray(hl.tstr))
        ),
    )
    return ht.annotate(
        family_stats=ht.family_stats.map(
            lambda fs: fs.drop('family_guid'),
        ),
    )


def remove_new_callset_family_guids(
    ht: hl.Table,
    project_guid: str,
    family_guids: list[str],
) -> hl.Table:
    if hl.eval(~ht.updates.project_guid.contains(project_guid)):
        return ht
    family_indexes_to_keep = [
        i
        for i, f in enumerate(hl.eval(ht.globals.family_guids[project_guid]))
        if f not in family_guids
    ]
    return ht.annotate(
        family_gt_stats=(
            {
                project_guid: (
                    hl.array(family_indexes_to_keep).map(
                        lambda i: ht.family_gt_stats[project_guid][i],
                    )
                    if len(family_indexes_to_keep) > 0
                    else hl.missing(ht.family_gt_stats[project_guid].dtype.element_type)
                ),
            }
        ),
    )


def join_sample_lookup_hts(
    dataset_type: DatasetType,
    sample_lookup_ht: hl.Table,
    callset_sample_lookup_ht: hl.Table,
    project_guid: str,
) -> hl.Table:
    sample_lookup_ht = sample_lookup_ht.join(callset_sample_lookup_ht, 'outer')
    first_field_name = next(
        iter(dataset_type.sample_lookup_table_fields_and_genotype_filter_fns.keys()),
    )
    empty_entry = hl.Struct(
        **{
            project_guid: hl.empty_set(hl.tstr)
            for project_guid in sample_lookup_ht[first_field_name].dtype.fields
        },
    )
    return sample_lookup_ht.select(
        **{
            field: hl.or_else(sample_lookup_ht[field], empty_entry).annotate(
                **{
                    project_guid: (
                        sample_lookup_ht[field]
                        .get(project_guid, hl.empty_set(hl.tstr))
                        .union(sample_lookup_ht[f'{field}_1'])
                    ),
                },
            )
            for field in dataset_type.sample_lookup_table_fields_and_genotype_filter_fns
        },
    )
