import hail as hl

from v03_pipeline.lib.model import ReferenceGenome


def select_fields_v2(ht: hl.Table) -> hl.Table:
    selects = {}
    global_idx = hl.eval(ht.globals.freq_index_dict['gnomad'])
    selects['AF'] = hl.float32(ht.freq[global_idx].AF)
    selects['AN'] = ht.freq[global_idx].AN
    selects['AC'] = ht.freq[global_idx].AC
    selects['Hom'] = ht.freq[global_idx].homozygote_count

    selects['AF_POPMAX_OR_GLOBAL'] = hl.float32(
        hl.or_else(
            ht.popmax[ht.globals.popmax_index_dict['gnomad']].AF,
            ht.freq[global_idx].AF,
        ),
    )
    selects['FAF_AF'] = hl.float32(ht.faf[ht.globals.popmax_index_dict['gnomad']].faf95)
    selects['Hemi'] = hl.if_else(
        ht.locus.in_autosome_or_par(),
        0,
        ht.freq[ht.globals.freq_index_dict['gnomad_male']].AC,
    )
    return ht.select(**selects)


def select_fields_v4(ht: hl.Table) -> hl.Table:
    selects = {}
    global_idx = hl.eval(ht.globals.freq_index_dict['adj'])
    selects['AF'] = hl.float32(ht.freq[global_idx].AF)
    selects['AN'] = ht.freq[global_idx].AN
    selects['AC'] = ht.freq[global_idx].AC
    selects['Hom'] = ht.freq[global_idx].homozygote_count

    grpmax_af = ht.grpmax['gnomad'].AF if hasattr(ht.grpmax, 'gnomad') else ht.grpmax.AF
    selects['AF_POPMAX_OR_GLOBAL'] = hl.float32(
        hl.or_else(grpmax_af, ht.freq[global_idx].AF),
    )
    selects['FAF_AF'] = hl.float32(ht.faf[ht.globals.faf_index_dict['adj']].faf95)
    selects['Hemi'] = hl.if_else(
        ht.locus.in_autosome_or_par(),
        0,
        ht.freq[ht.globals.freq_index_dict['XY_adj']].AC,
    )
    return ht.select(**selects)


def get_ht(raw_dataset_path: str, reference_genome: ReferenceGenome) -> hl.Table:
    ht = hl.read_table(raw_dataset_path)
    if reference_genome == ReferenceGenome.GRCh37:
        return select_fields_v2(ht)
    return select_fields_v4(ht)
