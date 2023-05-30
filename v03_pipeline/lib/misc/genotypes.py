import hail as hl

N_ALT_REF = 0
N_ALT_HET = 1
N_ALT_HOM = 2


def compute_sample_lookup_ht(mt: hl.MatrixTable) -> hl.Table:
    sample_ids = hl.agg.collect_as_set(mt.s)
    return mt.select_rows(
        no_call_samples=hl.agg.filter(~hl.is_defined(mt.GT), sample_ids),
        ref_samples=hl.agg.filter(mt.GT.n_alt_alleles() == N_ALT_REF, sample_ids),
        het_samples=hl.agg.filter(mt.GT.n_alt_alleles() == N_ALT_HET, sample_ids),
        hom_samples=hl.agg.filter(
            mt.GT.n_alt_alleles() == N_ALT_HOM,
            sample_ids,
        ),
    ).rows()


def remove_callset_sample_ids(genotypes_ht: hl.Table, samples_ht: hl.Table) -> hl.Table:
    sample_ids = samples_ht.aggregate(hl.agg.collect_as_set(samples_ht.s))
    return genotypes_ht.select(
        no_call_samples=genotypes_ht.no_call_samples.difference(sample_ids),
        ref_samples=genotypes_ht.ref_samples.difference(sample_ids),
        het_samples=genotypes_ht.het_samples.difference(sample_ids),
        hom_samples=genotypes_ht.hom_samples.difference(sample_ids),
    )


def union_sample_lookup_hts(
    sample_lookup_ht: hl.Table,
    callset_sample_lookup_ht: hl.Table,
) -> hl.Table:
    sample_lookup_ht = sample_lookup_ht.join(callset_sample_lookup_ht, 'outer')
    return sample_lookup_ht.select(
        no_call_samples=sample_lookup_ht.no_call_samples.union(
            sample_lookup_ht.no_call_samples_1,
        ),
        ref_samples=sample_lookup_ht.ref_samples.union(sample_lookup_ht.ref_samples_1),
        het_samples=sample_lookup_ht.het_samples.union(sample_lookup_ht.het_samples_1),
        hom_samples=sample_lookup_ht.hom_samples.union(sample_lookup_ht.hom_samples_1),
    )
