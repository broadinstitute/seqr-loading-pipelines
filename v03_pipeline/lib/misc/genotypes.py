import hail as hl


def compute_callset_genotypes(mt: hl.MatrixTable) -> hl.Table:
    sample_ids = hl.agg.collect_as_set(mt.s)
    return mt.select_rows(
        no_call_samples=hl.agg.filter(~hl.is_defined(mt.GT), sample_ids),
        ref_samples=hl.agg.filter(mt.GT.n_alt_alleles() == 0, sample_ids),
        het_samples=hl.agg.filter(mt.GT.n_alt_alleles() == 1, sample_ids),
        hom_samples=hl.agg.filter(
            mt.GT.n_alt_alleles() == 2, sample_ids, # noqa: PLR2004
        ),
    ).rows()


def remove_existing_calls(genotypes_ht: hl.Table, samples_ht: hl.Table) -> hl.Table:
    sample_ids = samples_ht.aggregate(hl.agg.collect_as_set(samples_ht.s))
    return genotypes_ht.select(
        no_call_samples=genotypes_ht.no_call_samples.difference(sample_ids),
        ref_samples=genotypes_ht.ref_samples.difference(sample_ids),
        het_samples=genotypes_ht.het_samples.difference(sample_ids),
        hom_samples=genotypes_ht.hom_samples.difference(sample_ids),
    )


def union_genotypes_hts(
    genotypes_ht: hl.Table,
    callset_genotypes_ht: hl.Table,
) -> hl.Table:
    joined_ht = genotypes_ht.join(callset_genotypes_ht, 'outer')
    return joined_ht.select(
        no_call_samples=joined_ht.no_call_samples.union(joined_ht.no_call_samples_1),
        ref_samples=joined_ht.ref_samples.union(joined_ht.ref_samples_1),
        het_samples=joined_ht.het_samples.union(joined_ht.het_samples_1),
        hom_samples=joined_ht.hom_samples.union(joined_ht.hom_samples_1),
    )
