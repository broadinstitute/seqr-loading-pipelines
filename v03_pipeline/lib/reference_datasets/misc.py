import contextlib
import os
import shutil
import tempfile
import zipfile

import hail as hl
import requests

from v03_pipeline.lib.model.definitions import ReferenceGenome


def get_enum_select_fields(ht: hl.Table, enums: dict | None) -> dict[str, hl.Expression]:
    enum_select_fields = {}
    for field_name, values in (enums or {}).items():
        if not hasattr(ht, field_name):
            if hasattr(ht, f'{field_name}_id') or hasattr(ht, f'{field_name}_ids'):
                continue
            else:
                raise ValueError(f'Unused enum {field_name}')

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


def filter_contigs(ht, reference_genome: ReferenceGenome):
    return ht.filter(
        hl.set(reference_genome.standard_contigs).contains(ht.locus.contig),
    )


def vcf_to_ht(file_name: str, reference_genome: ReferenceGenome) -> hl.Table:
    return hl.import_vcf(
        file_name,
        reference_genome=reference_genome.value,
        drop_samples=True,
        skip_invalid_loci=True,
        contig_recoding=reference_genome.contig_recoding(include_mt=True),
        force_bgz=True,
    ).rows()


def key_by_locus_alleles(ht: hl.Table, reference_genome: ReferenceGenome) -> hl.Table:
    chrom = (
        hl.format('chr%s', ht.chrom)
        if reference_genome == ReferenceGenome.GRCh38
        else ht.chrom
    )
    ht = ht.transmute(
        locus=hl.locus(chrom, ht.pos, reference_genome.value),
        alleles=hl.array([ht.ref, ht.alt]),
    )
    return ht.key_by('locus', 'alleles')


@contextlib.contextmanager
def download_zip_file(url, suffix='.zip'):
    with tempfile.NamedTemporaryFile(
        suffix=suffix,
    ) as tmp_file, requests.get(url, stream=True, timeout=10) as r:
        shutil.copyfileobj(r.raw, tmp_file)
        with zipfile.ZipFile(tmp_file.name, 'r') as zipf:
            zipf.extractall(os.path.dirname(tmp_file.name))
        # Extracting the zip file
        yield os.path.dirname(tmp_file.name)
