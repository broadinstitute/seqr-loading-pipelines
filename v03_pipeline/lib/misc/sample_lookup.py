import hail as hl


def compute_callset_sample_lookup_ht(mt: hl.MatrixTable) -> hl.Table:
    sample_ids = hl.agg.collect_as_set(mt.s)
    return mt.select_rows(
        ref_samples=hl.agg.filter(mt.GT.is_hom_ref(), sample_ids),
        het_samples=hl.agg.filter(mt.GT.is_het(), sample_ids),
        hom_samples=hl.agg.filter(mt.GT.is_hom_var(), sample_ids),
    ).rows()


def filter_callset_sample_ids(
    sample_lookup_ht: hl.Table,
    sample_subset_ht: hl.Table,
    project_guid: str,
) -> hl.Table:
    if hl.eval(~sample_lookup_ht.updates.project_guid.contains(project_guid)):
        return sample_lookup_ht
    sample_ids = sample_subset_ht.aggregate(hl.agg.collect_as_set(sample_subset_ht.s))
    fields = ['ref_samples', 'het_samples', 'hom_samples']
    return sample_lookup_ht.select(
        **{
            field: sample_lookup_ht[field].annotate(
                **{
                    project_guid: sample_lookup_ht[field][project_guid].difference(
                        sample_ids,
                    ),
                },
            )
            for field in fields
        },
    )


def join_sample_lookup_hts(
    sample_lookup_ht: hl.Table,
    callset_sample_lookup_ht: hl.Table,
    project_guid: str,
) -> hl.Table:
    sample_lookup_ht = sample_lookup_ht.join(callset_sample_lookup_ht, 'outer')
    fields = ['ref_samples', 'het_samples', 'hom_samples']
    # For rows that are "missing" in the existing sample lookup table,
    # replace the "missing" with an empty struct with
    # all projects (except for this one) as empty sets.
    # It was easier to reason about this as a separate annotation pass
    # than combined as a single annotate call
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
            for field in fields
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
            for field in fields
        },
    )
