import hail as hl


def compute_callset_sample_lookup_ht(mt: hl.MatrixTable) -> hl.Table:
    sample_ids = hl.agg.collect_as_set(mt.s)
    return mt.select_rows(
        ref_samples=hl.agg.filter(mt.GT.is_hom_ref(), sample_ids),
        het_samples=hl.agg.filter(mt.GT.is_het(), sample_ids),
        hom_samples=hl.agg.filter(mt.GT.is_hom_var(), sample_ids),
    ).rows()


def remove_callset_sample_ids(
    sample_lookup_ht: hl.Table,
    sample_subset_ht: hl.Table,
    project_guid: str,
) -> hl.Table:
    if hl.eval(~sample_lookup_ht.updates.project_guid.contains(project_guid)):
        return sample_lookup_ht
    sample_ids = sample_subset_ht.aggregate(hl.agg.collect_as_set(sample_subset_ht.s))
    return sample_lookup_ht.select(
        ref_samples=sample_lookup_ht.ref_samples.annotate(
            **{
                project_guid: sample_lookup_ht.ref_samples[project_guid].difference(
                    sample_ids,
                ),
            },
        ),
        het_samples=sample_lookup_ht.het_samples.annotate(
            **{
                project_guid: sample_lookup_ht.het_samples[project_guid].difference(
                    sample_ids,
                ),
            },
        ),
        hom_samples=sample_lookup_ht.hom_samples.annotate(
            **{
                project_guid: sample_lookup_ht.hom_samples[project_guid].difference(
                    sample_ids,
                ),
            },
        ),
    )


def union_sample_lookup_hts(
    sample_lookup_ht: hl.Table,
    callset_sample_lookup_ht: hl.Table,
    project_guid: str,
) -> hl.Table:
    sample_lookup_ht = sample_lookup_ht.join(callset_sample_lookup_ht, 'outer')
    for field in ['ref_samples', 'het_samples', 'hom_samples']:
        # If the row exists in the new callset but not the existing sample lookup table
        # we need to materialize the missing struct of projects w/ empty sets, else
        # the "missing" propates and gobbles up the "union" below.
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
                ),
            },
        )
        sample_lookup_ht = sample_lookup_ht.annotate(
            **{
                field: sample_lookup_ht[field].annotate(
                    **{
                        project_guid: (
                            sample_lookup_ht[field]
                            .get(project_guid, hl.empty_set(hl.tstr))
                            .union(sample_lookup_ht[f'{field}_1'])
                        ),
                    },
                ),
            },
        )
        sample_lookup_ht = sample_lookup_ht.drop(f'{field}_1')
    return sample_lookup_ht
