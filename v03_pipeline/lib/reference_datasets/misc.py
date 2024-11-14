import os
import shutil
import tempfile
import zipfile

from contextlib import contextmanager
import hail as hl
import requests

from v03_pipeline.lib.model.definitions import ReferenceGenome


def get_enum_select_fields(ht: hl.Table, enums: dict) -> dict[str, hl.Expression]:
    enum_select_fields = {}
    for field_name, values in enums.items():
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


def key_by_locus_alleles(ht: hl.Table, reference_genome: ReferenceGenome) -> hl.Table:
    chrom = (
        hl.format('chr%s', ht.chrom)
        if reference_genome == ReferenceGenome.GRCh38
        else ht.chrom
    )
    locus = hl.locus(chrom, ht.pos, reference_genome.value)
    alleles = hl.array([ht.ref, ht.alt])
    ht = ht.transmute(locus=locus, alleles=alleles)
    return ht.key_by('locus', 'alleles')


@contextmanager
def download_zip_file(url, suffix='.zip'):
    extracted_filename = url.removesuffix('.zip').split('/')[-1]
    with tempfile.NamedTemporaryFile(
        suffix=suffix,
    ) as tmp_file, requests.get(url, stream=True, timeout=10) as r:
        shutil.copyfileobj(r.raw, tmp_file)
        with zipfile.ZipFile(tmp_file.name, 'r') as zipf:
            zipf.extractall(os.path.dirname(tmp_file.name))
        yield os.path.join(
            # Extracting the zip file
            os.path.dirname(tmp_file.name),
            extracted_filename,
        )
