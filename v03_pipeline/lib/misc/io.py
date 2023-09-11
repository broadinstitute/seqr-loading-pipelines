from __future__ import annotations

import os
import uuid

import hail as hl

from v03_pipeline.lib.misc.gcnv import parse_gcnv_genes
from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome, SampleType

BIALLELIC = 2


def does_file_exist(path: str) -> bool:
    if path.startswith('gs://'):
        return hl.hadoop_exists(path)
    return os.path.exists(path)


def split_multi_hts(mt: hl.MatrixTable) -> hl.MatrixTable:
    bi = mt.filter_rows(hl.len(mt.alleles) == BIALLELIC)
    bi = bi.annotate_rows(a_index=1, was_split=False)
    multi = mt.filter_rows(hl.len(mt.alleles) > BIALLELIC)
    split = hl.split_multi_hts(multi)
    return split.union_rows(bi)


def import_gcnv_bed_file(callset_path: str) -> hl.MatrixTable:
    # Hail falls over itself with OOMs with use_new_shuffle here... no clue why.
    hl._set_flags(use_new_shuffle=None, no_whole_stage_codegen='1')  # noqa: SLF001
    ht = hl.import_table(
        callset_path,
        types={
            'start': hl.tint32,
            'end': hl.tint32,
            'CN': hl.tint32,
            'QS': hl.tint32,
            'defragmented': hl.tbool,
            'sf': hl.tfloat64,
            'sc': hl.tint32,
            'genes_any_overlap_totalExons': hl.tint32,
            'genes_strict_overlap_totalExons': hl.tint32,
            'no_ovl': hl.tbool,
            'is_latest': hl.tbool,
        },
        min_partitions=500,
        force=callset_path.endswith('gz'),
    )
    mt = ht.to_matrix_table(
        row_key=['variant_name', 'svtype'],
        col_key=['sample_fix'],
        row_fields=['chr', 'sc', 'sf', 'strvctvre_score'],
    )
    mt = mt.rename({'start': 'sample_start', 'end': 'sample_end'})
    mt = mt.key_cols_by(s=mt.sample_fix)
    mt = mt.annotate_rows(
        variant_id=hl.format('%s_%s', mt.variant_name, mt.svtype),
        filters=hl.empty_set(hl.tstr),
        start=hl.agg.min(mt.sample_start),
        end=hl.agg.max(mt.sample_end),
        num_exon=hl.agg.max(mt.genes_any_overlap_totalExons),
        gene_ids=hl.flatten(
            hl.agg.collect_as_set(parse_gcnv_genes(mt.genes_any_overlap_Ensemble_ID)),
        ),
        cg_genes=hl.flatten(
            hl.agg.collect_as_set(parse_gcnv_genes(mt.genes_CG_Ensemble_ID)),
        ),
        lof_genes=hl.flatten(
            hl.agg.collect_as_set(parse_gcnv_genes(mt.genes_LOF_Ensemble_ID)),
        ),
    )
    mt = mt.unfilter_entries()
    return mt


def import_vcf(
    callset_path: str,
    reference_genome: ReferenceGenome,
) -> hl.MatrixTable:
    # Import the VCFs from inputs. Set min partitions so that local pipeline execution takes advantage of all CPUs.
    recode = {}
    if reference_genome == ReferenceGenome.GRCh38:
        recode = {f'{i}': f'chr{i}' for i in ([*list(range(1, 23)), 'X', 'Y'])}
    else:
        recode = {f'chr{i}': f'{i}' for i in ([*list(range(1, 23)), 'X', 'Y'])}
    return hl.import_vcf(
        callset_path,
        reference_genome=reference_genome.value,
        skip_invalid_loci=True,
        contig_recoding=recode,
        force_bgz=True,
        min_partitions=500,
        find_replace=('nul', '.'),
    )


def import_callset(
    callset_path: str,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    filters_path: str | None = None,
) -> hl.MatrixTable:
    if dataset_type == DatasetType.GCNV:
        mt = import_gcnv_bed_file(callset_path)
    elif 'vcf' in callset_path:
        mt = import_vcf(callset_path, reference_genome)
    elif 'mt' in callset_path:
        mt = hl.read_matrix_table(callset_path)
    if dataset_type == DatasetType.SNV_INDEL:
        mt = split_multi_hts(mt)
    if dataset_type == DatasetType.SV:
        mt = mt.annotate_rows(variant_id=mt.rsid)
    if filters_path:
        filters_ht = import_vcf(filters_path, reference_genome).rows()
        mt = mt.annotate_rows(filters=filters_ht[mt.row_key].filters)
    mt = mt.key_rows_by(*dataset_type.table_key_type(reference_genome).fields)
    mt = mt.select_globals()
    mt = mt.select_rows(*dataset_type.row_fields)
    mt = mt.select_cols(*dataset_type.col_fields)
    return mt.select_entries(*dataset_type.entries_fields)


def import_remap(remap_path: str) -> hl.Table:
    ht = hl.import_table(remap_path)
    ht = ht.select(
        s=ht.s,
        seqr_id=ht.seqr_id,
    )
    return ht.key_by(ht.s)


def import_pedigree(pedigree_path: str) -> hl.Table:
    ht = hl.import_table(pedigree_path)
    ht = ht.select(
        family_guid=ht.Family_GUID,
        s=ht.Individual_ID,
    )
    return ht.key_by(ht.family_guid)


def write(
    t: hl.Table | hl.MatrixTable,
    destination_path: str,
    checkpoint: bool = True,
    n_partitions: int | None = None,
) -> hl.Table | hl.MatrixTable:
    suffix = 'mt' if isinstance(t, hl.MatrixTable) else 'ht'
    if checkpoint:
        t = t.checkpoint(
            os.path.join(
                Env.HAIL_TMPDIR,
                f'{uuid.uuid4()}.{suffix}',
            ),
        )
    # "naive_coalesce" will decrease parallelism of hail's pipelined operations
    # , so we sneak this re-partitioning until after the checkpoint.
    if n_partitions:
        t = t.naive_coalesce(n_partitions)
    return t.write(destination_path, overwrite=True, stage_locally=True)
