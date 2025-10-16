import contextlib
import os
import random
import string
import tempfile
import zipfile

import hail as hl
import hailtop.fs as hfs
import requests

from v03_pipeline.lib.core.dataset_type import DatasetType
from v03_pipeline.lib.core.definitions import ReferenceGenome
from v03_pipeline.lib.core.environment import Env
from v03_pipeline.lib.misc.io import split_multi_hts

BIALLELIC = 2


def compress_floats(ht: hl.Table):
    # Parse float64s into float32s to save space!
    return ht.select(
        **{
            k: hl.float32(v) if v.dtype == hl.tfloat64 else v
            for k, v in ht.row_value.items()
        },
    )


def generate_random_string(length=5):
    """Generates a random string of the specified length."""
    letters = string.ascii_letters + string.digits
    return ''.join(random.choice(letters) for i in range(length))  # noqa: S311


def get_enum_select_fields(
    ht: hl.Table,
    enums: dict | None,
) -> dict[str, hl.Expression]:
    enum_select_fields = {}
    for field_name, values in (enums or {}).items():
        if not hasattr(ht, field_name):
            if hasattr(ht, f'{field_name}_id') or hasattr(ht, f'{field_name}_ids'):
                continue
            error = f'Unused enum {field_name}'
            raise ValueError(error)

        lookup = hl.dict(
            hl.enumerate(values, index_first=False).extend(
                # NB: adding missing values here allows us to
                # hard fail if a mapped key is present and has an unexpected value
                # but propagate missing values.
                [(hl.missing(hl.tstr), hl.missing(hl.tint32))],
            ),
        )
        # NB: this conditioning on type is "outside" the hail expression context.
        if (
            isinstance(ht[field_name].dtype, hl.tarray | hl.tset)
            and ht[field_name].dtype.element_type == hl.tstr
        ):
            enum_select_fields[f'{field_name}_ids'] = ht[field_name].map(
                lambda x: lookup[x],  # noqa: B023
            )
        else:
            enum_select_fields[f'{field_name}_id'] = lookup[ht[field_name]]
    return enum_select_fields


def filter_mito_contigs(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    ht: hl.Table,
) -> hl.Table:
    if dataset_type == DatasetType.MITO:
        return ht.filter(ht.locus.contig == reference_genome.mito_contig)
    return ht.filter(ht.locus.contig != reference_genome.mito_contig)


def filter_contigs(ht, reference_genome: ReferenceGenome):
    if hasattr(ht, 'interval'):
        return ht.filter(
            hl.set(reference_genome.standard_contigs).contains(
                ht.interval.start.contig,
            ),
        )
    # SV reference datasets are not keyed by locus.
    if hasattr(ht, 'locus'):
        return ht.filter(
            hl.set(reference_genome.standard_contigs).contains(ht.locus.contig),
        )
    return ht


def vcf_to_ht(
    file_name: str,
    reference_genome: ReferenceGenome,
    split_multi=False,
) -> hl.Table:
    mt = hl.import_vcf(
        file_name,
        reference_genome=reference_genome.value,
        drop_samples=True,
        skip_invalid_loci=True,
        contig_recoding=reference_genome.contig_recoding(include_mt=True),
        force_bgz=True,
        array_elements_required=False,
    )
    if split_multi:
        return split_multi_hts(mt, True).rows()

    # Validate that there exist no multialellic variants in the table.
    count_non_biallelic = mt.aggregate_rows(
        hl.agg.count_where(hl.len(mt.alleles) > BIALLELIC),
    )
    if count_non_biallelic:
        error = f'Encountered {count_non_biallelic} multiallelic variants'
        raise ValueError(error)
    return mt.rows()


def key_by_locus_alleles(ht: hl.Table, reference_genome: ReferenceGenome) -> hl.Table:
    chrom = (
        hl.format('chr%s', ht.chrom)
        if reference_genome == ReferenceGenome.GRCh38
        else hl.if_else(ht.chrom == 'M', 'MT', ht.chrom)
    )
    ht = ht.transmute(
        locus=hl.locus(chrom, ht.pos, reference_genome.value),
        alleles=hl.array([ht.ref, ht.alt]),
    )
    return ht.key_by('locus', 'alleles')


def copyfileobj(fsrc, fdst, decode_content, length=16 * 1024):
    """Copy data from file-like object fsrc to file-like object fdst."""
    while True:
        buf = fsrc.read(length, decode_content=decode_content)
        if not buf:
            break
        fdst.write(buf)


@contextlib.contextmanager
def download_zip_file(url, dataset_name: str, suffix='.zip', decode_content=False):
    dir_ = f'/tmp/{generate_random_string()}/{dataset_name}'  # noqa: S108
    os.makedirs(dir_, exist_ok=True)
    with (
        tempfile.NamedTemporaryFile(
            dir=dir_,
            suffix=suffix,
        ) as tmp_file,
        requests.get(url, stream=True, timeout=10) as r,
    ):
        copyfileobj(r.raw, tmp_file, decode_content)
        with zipfile.ZipFile(tmp_file.name, 'r') as zipf:
            zipf.extractall(dir_)
        yield copy_to_cloud_storage(dir_)


def select_for_interval_reference_dataset(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    additional_selects: dict,
    chrom_field: str = 'chrom',
    start_field: str = 'start',
    end_field: str = 'end',
) -> hl.Table:
    ht = ht.select(
        interval=hl.locus_interval(
            ht[chrom_field],
            ht[start_field] + 1,
            ht[end_field] + 1,
            reference_genome=reference_genome.value,
            invalid_missing=True,
        ),
        **additional_selects,
    )
    return ht.key_by('interval')


def copy_to_cloud_storage(file_name: str) -> str:
    if not Env.HAIL_TMP_DIR.startswith('gs://'):
        return file_name
    if os.path.isdir(file_name):
        cloud_storage_path = os.path.join(Env.HAIL_TMP_DIR, file_name.lstrip('/'))
    else:
        cloud_storage_path = os.path.join(Env.HAIL_TMP_DIR, os.path.basename(file_name))
    hfs.copy(file_name, cloud_storage_path)
    return cloud_storage_path
