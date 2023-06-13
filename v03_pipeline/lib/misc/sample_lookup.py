import hail as hl


def compute_sample_lookup_ht(mt: hl.MatrixTable, project_guid: str) -> hl.Table:
    sample_ids = hl.agg.collect_as_set(mt.s)
    return mt.select_rows(
        ref_samples=hl.Struct(**{project_guid: hl.agg.filter(mt.GT.is_hom_ref(), sample_ids)}),
        het_samples=hl.Struct(**{project_guid: hl.agg.filter(mt.GT.is_het(), sample_ids)}),
        hom_samples=hl.Struct(**{project_guid: hl.agg.filter(mt.GT.is_hom_var(), sample_ids)}),
    ).rows()


def remove_callset_sample_ids(
    sample_lookup_ht: hl.Table,
    sample_subset_ht: hl.Table,
    project_guid: str,
) -> hl.Table:
    sample_ids = sample_subset_ht.aggregate(hl.agg.collect_as_set(sample_subset_ht.s))
    return sample_lookup_ht.select(
        ref_samples=sample_lookup_ht.ref_samples.annotate(
            **{project_guid: sample_lookup_ht.ref_samples[project_guid].difference(sample_ids)}
        ),
        het_samples=sample_lookup_ht.het_samples.annotate(
            **{project_guid: sample_lookup_ht.het_samples[project_guid].difference(sample_ids)}
        ),
        hom_samples=sample_lookup_ht.hom_samples.annotate(
            **{project_guid: sample_lookup_ht.hom_samples[project_guid].difference(sample_ids)}
        ),
    )

def _union_no_missings(expr1: hl.SetExpression, expr2: hl.SetExpression):
    # By default, the set union operator when peformed with a missing set returns the missing set.
    # Instead, we'd like to keep the non-missing set if possible.
    return (
        hl.case()
        .when(hl.is_missing(expr1), expr2)
        .when(hl.is_missing(expr2), expr1)
        .default(expr1.union(expr2))
    )

def union_sample_lookup_hts(
    sample_lookup_ht: hl.Table,
    callset_sample_lookup_ht: hl.Table,
    project_guid: str,
) -> hl.Table:
    sample_lookup_ht = sample_lookup_ht.join(callset_sample_lookup_ht, 'outer')
    return sample_lookup_ht.select(
        ref_samples=sample_lookup_ht.ref_samples.annotate(
            **{project_guid: _union_no_missings(sample_lookup_ht.ref_samples[project_guid], sample_lookup_ht.ref_samples_1[project_guid])}
        ),
        het_samples=sample_lookup_ht.het_samples.annotate(
            **{project_guid: _union_no_missings(sample_lookup_ht.het_samples[project_guid], sample_lookup_ht.het_samples_1[project_guid])}
        ),
        hom_samples=sample_lookup_ht.hom_samples.annotate(
            **{project_guid: _union_no_missings(sample_lookup_ht.hom_samples[project_guid], sample_lookup_ht.hom_samples_1[project_guid])}
        ),
    )
