from __future__ import annotations

import os
import tempfile
import uuid

import hail as hl

from v03_pipeline.lib.model import DataRoot, DatasetType, Env, ReferenceGenome

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
    # TODO implement me.
    # also remember to annotate pos = hl.agg.min(mt.sample_start)
    return hl.import_table(callset_path)


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
    )


def import_callset(
    callset_path: str,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> hl.MatrixTable:
    if dataset_type == DatasetType.GCNV:
        return import_gcnv_bed_file(callset_path)
    mt = import_vcf(callset_path, reference_genome)
    if dataset_type == DatasetType.SNV:
        mt = split_multi_hts(mt)
    key_type = dataset_type.table_key_type(reference_genome)
    mt = mt.key_rows_by(*key_type.fields)
    mt = mt.select_globals()
    mt = mt.select_rows(*dataset_type.row_fields)
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
    env: Env,
    t: hl.Table | hl.MatrixTable,
    destination_path: str,
    checkpoint: bool = True,
    n_partitions: int | None = None,
) -> hl.Table | hl.MatrixTable:
    suffix = 'mt' if isinstance(t, hl.MatrixTable) else 'ht'
    if checkpoint and (env == Env.LOCAL or env == Env.TEST):
        with tempfile.TemporaryDirectory() as d:
            t = t.checkpoint(
                os.path.join(
                    d,
                    f'{uuid.uuid4()}.{suffix}',
                ),
            )
            return t.write(destination_path, overwrite=True, stage_locally=True)
    elif checkpoint:
        t = t.checkpoint(
            os.path.join(
                DataRoot.SEQR_SCRATCH_TEMP.value,
                f'{uuid.uuid4()}.{suffix}',
            ),
        )
    # "naive_coalesce" will decrease parallelism of hail's pipelined operations
    # , so we sneak this re-partitioning until after the checkpoint.
    if n_partitions:
        t = t.naive_coalesce(n_partitions)
    return t.write(destination_path, overwrite=True, stage_locally=True)
