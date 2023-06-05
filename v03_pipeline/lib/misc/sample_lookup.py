import hail as hl


def compute_sample_lookup_ht(mt: hl.MatrixTable) -> hl.Table:
    sample_ids = hl.agg.collect_as_set(mt.s)
    return mt.select_rows(
        ref_samples=hl.agg.filter(mt.GT.is_hom_ref(), sample_ids),
        het_samples=hl.agg.filter(mt.GT.is_het(), sample_ids),
        hom_samples=hl.agg.filter(mt.GT.is_hom_var(), sample_ids),
    ).rows()


def remove_callset_sample_ids(
    sample_lookup_ht: hl.Table,
    sample_subset_ht: hl.Table,
) -> hl.Table:
    sample_ids = sample_subset_ht.aggregate(hl.agg.collect_as_set(sample_subset_ht.s))
    return sample_lookup_ht.select(
        ref_samples=sample_lookup_ht.ref_samples.difference(sample_ids),
        het_samples=sample_lookup_ht.het_samples.difference(sample_ids),
        hom_samples=sample_lookup_ht.hom_samples.difference(sample_ids),
    )


def union_sample_lookup_hts(
    sample_lookup_ht: hl.Table,
    callset_sample_lookup_ht: hl.Table,
) -> hl.Table:
    sample_lookup_ht = sample_lookup_ht.join(callset_sample_lookup_ht, 'outer')
    return sample_lookup_ht.select(
        ref_samples=sample_lookup_ht.ref_samples.union(sample_lookup_ht.ref_samples_1),
        het_samples=sample_lookup_ht.het_samples.union(sample_lookup_ht.het_samples_1),
        hom_samples=sample_lookup_ht.hom_samples.union(sample_lookup_ht.hom_samples_1),
    )
