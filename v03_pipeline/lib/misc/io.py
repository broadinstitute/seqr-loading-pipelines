import hashlib
import math
import os
import uuid

import hail as hl
import hailtop.fs as hfs

from v03_pipeline.lib.misc.gcnv import parse_gcnv_genes
from v03_pipeline.lib.misc.nested_field import parse_nested_field
from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome, Sex

BIALLELIC = 2
B_PER_MB = 1 << 20  # 1024 * 1024
MB_PER_PARTITION = 128
MAX_SAMPLES_SPLIT_MULTI_SHUFFLE = 100

MALE = 'Male'
FEMALE = 'Female'


def does_file_exist(path: str) -> bool:
    if path.startswith('gs://'):
        return hl.hadoop_exists(path)
    return os.path.exists(path)


def file_size_bytes(path: str) -> int:
    size_bytes = 0
    seen_files = set()
    while True:
        files = hl.hadoop_ls(path)
        has_directory = False
        for f in files:
            if f['path'] in seen_files:
                continue
            if f['is_dir']:
                has_directory = True
                continue
            size_bytes += f['size_bytes']
            seen_files.add(f['path'])
        if not has_directory:
            break
        path = os.path.join(path, '**')
    return size_bytes


def compute_hail_n_partitions(file_size_b: int) -> int:
    return math.ceil(file_size_b / B_PER_MB / MB_PER_PARTITION)


def split_multi_hts(mt: hl.MatrixTable) -> hl.MatrixTable:
    bi = mt.filter_rows(hl.len(mt.alleles) == BIALLELIC)
    # split_multi_hts filters star alleles by default, but we
    # need that behavior for bi-allelic variants in addition to
    # multi-allelics
    bi = bi.filter_rows(~bi.alleles.contains('*'))
    bi = bi.annotate_rows(a_index=1, was_split=False)
    multi = mt.filter_rows(hl.len(mt.alleles) > BIALLELIC)
    split = hl.split_multi_hts(
        multi,
        permit_shuffle=mt.count()[1] < MAX_SAMPLES_SPLIT_MULTI_SHUFFLE,
    )
    mt = split.union_rows(bi)
    return mt.distinct_by_row()


def import_gcnv_bed_file(callset_path: str) -> hl.MatrixTable:
    # Hail falls over itself with OOMs with use_new_shuffle here... no clue why.
    hl._set_flags(use_new_shuffle=None, no_whole_stage_codegen='1')  # noqa: SLF001
    ht = hl.import_table(
        callset_path,
        types={
            **DatasetType.GCNV.col_fields,
            **DatasetType.GCNV.entries_fields,
            **DatasetType.GCNV.row_fields,
        },
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
    return mt.unfilter_entries()


def import_vcf(
    callset_path: str,
    reference_genome: ReferenceGenome,
) -> hl.MatrixTable:
    # Import the VCFs from inputs. Set min partitions so that local pipeline execution takes advantage of all CPUs.
    return hl.import_vcf(
        callset_path,
        reference_genome=reference_genome.value,
        skip_invalid_loci=True,
        contig_recoding=reference_genome.contig_recoding(),
        force_bgz=True,
        find_replace=(
            'nul',
            '.',
        ),  # Required for internal exome callsets (+ some AnVIL requests)
        array_elements_required=False,
        call_fields=[],  # PGT is unused downstream, but is occasionally present in old VCFs!
    )


def import_callset(
    callset_path: str,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> hl.MatrixTable:
    if dataset_type == DatasetType.GCNV:
        mt = import_gcnv_bed_file(callset_path)
    elif 'vcf' in callset_path:
        mt = import_vcf(callset_path, reference_genome)
    elif 'mt' in callset_path:
        mt = hl.read_matrix_table(callset_path)
    if dataset_type == DatasetType.SV:
        mt = mt.annotate_rows(variant_id=mt.rsid)
    return mt.key_rows_by(*dataset_type.table_key_type(reference_genome).fields)


def select_relevant_fields(
    mt: hl.MatrixTable,
    dataset_type: DatasetType,
    additional_row_fields: None | dict[str, hl.expr.types.HailType | set] = None,
) -> hl.MatrixTable:
    mt = mt.select_globals()
    mt = mt.select_rows(
        **{field: parse_nested_field(mt, field) for field in dataset_type.row_fields},
        **{
            field: parse_nested_field(mt, field)
            for field in (additional_row_fields or [])
        },
    )
    mt = mt.select_cols(
        **{field: parse_nested_field(mt, field) for field in dataset_type.col_fields},
    )
    return mt.select_entries(
        **{
            field: parse_nested_field(mt, field)
            for field in dataset_type.entries_fields
        },
    )


def import_imputed_sex(imputed_sex_path: str) -> hl.Table:
    ht = hl.import_table(imputed_sex_path)
    ht = ht.select(
        s=ht.collaborator_sample_id,
        predicted_sex=(
            hl.case()
            .when(ht.predicted_sex == FEMALE, Sex.FEMALE.value)
            .when(ht.predicted_sex == MALE, Sex.MALE.value)
            .or_error(
                hl.format(
                    'Found unexpected value %s in imputed sex file',
                    ht.predicted_sex,
                ),
            )
        ),
    )
    return ht.key_by(ht.s)


def import_remap(remap_path: str) -> hl.Table:
    ht = hl.import_table(remap_path)
    ht = ht.select(
        s=ht.s,
        seqr_id=ht.seqr_id,
    )
    return ht.key_by(ht.s)


def import_pedigree(pedigree_path: str) -> hl.Table:
    ht = hl.import_table(pedigree_path, missing='')
    return ht.select(
        sex=ht.Sex,
        family_guid=ht.Family_GUID,
        s=ht.Individual_ID,
        maternal_s=ht.Maternal_ID,
        paternal_s=ht.Paternal_ID,
    )

def remap_pedigree_hash(remap_path: str, pedigree_path: str) -> str:
    sha256 = hashlib.sha256()
    with hfs.open(remap_path) as f1:
        sha256.update(f1.read().encode('utf8'))
    with hfs.open(pedigree_path) as f2:
        sha256.update(f2.read().encode('utf8'))
    return sha256.hexdigest()[:32]

def checkpoint(t: hl.Table | hl.MatrixTable) -> tuple[hl.Table | hl.MatrixTable, str]:
    suffix = 'mt' if isinstance(t, hl.MatrixTable) else 'ht'
    read_fn = hl.read_matrix_table if isinstance(t, hl.MatrixTable) else hl.read_table
    checkpoint_path = os.path.join(
        Env.HAIL_TMPDIR,
        f'{uuid.uuid4()}.{suffix}',
    )
    t.write(checkpoint_path)
    return read_fn(checkpoint_path), checkpoint_path


def write(
    t: hl.Table | hl.MatrixTable,
    destination_path: str,
    repartition: bool = True,
) -> hl.Table | hl.MatrixTable:
    t, path = checkpoint(t)
    if repartition:
        t = t.repartition(
            compute_hail_n_partitions(file_size_bytes(path)),
            shuffle=False,
        )
    return t.write(destination_path, overwrite=True)
