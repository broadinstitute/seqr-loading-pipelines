import hail as hl

from v03_pipeline.lib.model import DatasetType


def compute_callset_sample_lookup_ht(
    dataset_type: DatasetType,
    mt: hl.MatrixTable,
) -> hl.Table:
    return mt.select_rows(
        **{
            field: hl.agg.filter(genotype_filter_fn(mt), hl.agg.collect_as_set(mt.s))
            for field, genotype_filter_fn in dataset_type.sample_lookup_table_fields_and_genotype_filter_fns.items()
        },
    ).rows()


def filter_callset_sample_ids(
    dataset_type: DatasetType,
    sample_lookup_ht: hl.Table,
    sample_subset_ht: hl.Table,
    project_guid: str,
) -> hl.Table:
    if hl.eval(~sample_lookup_ht.updates.project_guid.contains(project_guid)):
        return sample_lookup_ht
    sample_ids = sample_subset_ht.aggregate(hl.agg.collect_as_set(sample_subset_ht.s))
    return sample_lookup_ht.select(
        **{
            field: sample_lookup_ht[field].annotate(
                **{
                    project_guid: sample_lookup_ht[field][project_guid].difference(
                        sample_ids,
                    ),
                },
            )
            for field in dataset_type.sample_lookup_table_fields_and_filter_fns
        },
    )


def join_sample_lookup_hts(
    dataset_type: DatasetType,
    sample_lookup_ht: hl.Table,
    callset_sample_lookup_ht: hl.Table,
    project_guid: str,
) -> hl.Table:
    sample_lookup_ht = sample_lookup_ht.join(callset_sample_lookup_ht, 'outer')
    # For rows that are "missing" in the existing sample lookup table,
    # replace the "missing" with a non-empty struct with
    # all projects (except for this one) as empty sets.
    # It was easier to reason about this as a separate annotation pass
    # than combined with the below select.
    sample_lookup_ht = sample_lookup_ht.annotate(
        **{
            field: hl.or_else(
                sample_lookup_ht[field],
                hl.Struct(
                    **{
                        existing_project_guid: hl.empty_set(hl.tstr)
                        for existing_project_guid in sample_lookup_ht[
                            field
                        ].dtype.fields
                    },
                ),
            )
            for field in dataset_type.sample_lookup_table_fields_and_filter_fns
        },
    )
    return sample_lookup_ht.select(
        **{
            field: sample_lookup_ht[field].annotate(
                **{
                    project_guid: (
                        sample_lookup_ht[field]
                        .get(project_guid, hl.empty_set(hl.tstr))
                        .union(sample_lookup_ht[f'{field}_1'])
                    ),
                },
            )
            for field in dataset_type.sample_lookup_table_fields_and_filter_fns
        },
    )
