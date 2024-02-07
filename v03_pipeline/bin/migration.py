import hail as hl

from v03_pipeline.lib.misc.family_entries import globalize_ids
from v03_pipeline.lib.misc.io import import_pedigree
from v03_pipeline.lib.misc.pedigree import parse_pedigree_ht_to_families
from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome

# Need to use the GCP bucket as temp storage for very large callset joins
hl.init(tmp_dir='gs://seqr-scratch-temp', idempotent=True)

# Interval ref data join causes shuffle death, this prevents it
hl._set_flags(use_new_shuffle='1', no_whole_stage_codegen='1')  # noqa: SLF001
sc = hl.spark_context()
sc.addPyFile('gs://seqr-luigi/releases/dev/latest/pyscripts.zip')

def deglobalize_sample_ids(ht: hl.Table) -> hl.Table:
    ht = ht.annotate(
        entries=(
            hl.enumerate(ht.entries).starmap(
                lambda i, e: hl.Struct(**e, s=ht.sample_ids[i]),
            )
        ),
    )
    return ht.drop('sample_ids')


Env.HAIL_TMPDIR='gs://seqr-scratch-temp'
dataset_type = DatasetType.SNV_INDEL
reference_genome = ReferenceGenome.GRCh38
projects = [
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0208_kang_v11.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0211_guptill_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0213_lin_cohort_2s.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0234_mody_250s.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0238_inmr_wgs_pcr_free_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0244_dowling_v10.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0253_newcastle_v9.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0254_jueppner_3s_pcrfree_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0261_bonnemann_v9.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0276_inmr_neuromuscular_disea.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0279_laing_pcrfree_wgs_v5.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0280_cmg_hildebrandt_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0284_pierce_retinal_degenerat.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0285_cmg_gleeson_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0286_bonnemann_pcrfree_wgs_v5.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0292_gleeson_ciliopathies_exo.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0293_inmr_neuromuscular_disea.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0294_myoseq_v20.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0301_myoseq_pcrfree_wgs_v5.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0303_cmg_manton_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0308_henrickson_skin_11s.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0310_manton_pcrfree_4s.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0311_pierce_retinal_degenerat.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0312_cmg_bonnemann_genomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0315_coppens_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0316_cmg_kang_genomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0317_cmg_topf_bonn_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0330_cmg_laing_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0332_cmg_estonia_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0344_cmg_beggs_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0346_walsh_lab_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0351_cmg_bonnemann_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0354_cmg_hildebrandt_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0358_cmg_vcgs_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0359_cmg_manton_genomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0364_cmg_bodamer_manton_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0365_cmg_scott_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0367_cmg_seidman_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0369_cmg_ware_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0375_cmg_topf_tubitak_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0379_ogrady_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0380_cmg_gazda_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0381_manzini_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0382_cmg_myoseq_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0383_gazda_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0384_rare_genomes_project_gen.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0393_cmg_sankaran_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0394_cmg_kang_lgmd_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0401_cmg_scott_exomes_retrosp.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0404_cmg_estonia_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0405_cmg_vcgs_genomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0406_cmg_topf_panel_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0407_cmg_wendy_chung_genomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0408_cmg_perth_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0411_cmg_pcgc_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0416_cmg_topf_cms_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0419_gmkf_dsd_vilain.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0420_engle_wgs_900.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0421_bamforth.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0424_atgu_wgs_jueppner.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0432_engle_wgs_2_sample.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0433_cmg_hirschhorn_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0447_non_cmg_kang_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0449_pathways_mgh.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0450_rare_genomes_project_exo.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0451_cmg_walsh_exomes_microce.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0455_cmg_walsh_exomes_lispac.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0456_cmg_walsh_exomes_ch.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0469_cmg_engle.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0470_laing_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0474_cmg_sherr_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0475_cmg_tristani_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0476_cmg_wilkins_haug_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0477_cmg_sankaran_genomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0478_cmg_walsh_exomes_id.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0481_cmg_gleeson_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0482_cmg_thaker_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0483_cmg_pollak_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0484_cmg_sinclair_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0485_cmg_beggs_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0486_cmg_gcnv.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0487_cmg_myoseq_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0488_greka_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0489_cmg_sweetser_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0492_cmg_lerner_ellis_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0495_cmg_topf_ea_cms_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0496_cmg_fleming_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0497_cmg_muntoni_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0502_cmg_seidman_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0503_cmg_walsh_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0504_greka_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0517_cmg_uchicago_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0521_chan_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0523_cmg_manton_doose_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0524_cmg_wendy_chung_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0525_thaker_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0526_cmg_neurodev_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0528_cmg_engle_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0529_cmg_southampton_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0530_cmg_southampton_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0531_cmg_fajgenbaum.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0532_cmg_walsh_exomes_pmg.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0533_cmg_muntoni_external_exo.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0534_cmg_laing_ravencroft_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0535_cmg_sherr_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0536_cmg_muntoni_external_gen.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0537_cmg_scott_genomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0538_cmg_lerner_ellis_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0539_cmg_jueppner.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0540_mgh_pathways_probands_on.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0542_aicardi_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0543_aicardi_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0546_rare_genome_project_exte.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0548_bwh_pina_aguilar_dgap.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0549_cmg_roscioli_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0550_cmg_jueppner_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0552_mgrc_hufnagel_genomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0554_tgg_shimamura_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0555_seqr_demo.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0558_200716_201109_210127_210.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0559_mgrc_wilson_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0560_mgrc_thaker_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0562_mgrc_bonnemann_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0563_mgrc_sherr_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0565_mgrc_bonnemann_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0566_tgg_ravenscroft_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0567_tgg_myoseq_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0568_tgg_ravenscroft_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0571_gregor_thaker_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0575_cmg_alexander_ext_wgs_v2.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0576_amel_sims_40s_v2.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0579_wiggs_wes_variantcalling.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0582_uci_wes_n21.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0583_gregor_manton_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0584_chan_cohort_wes_330.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0585_210406_210506_210601_joi.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0586_gatk4_germline_preproces.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0587_gregor_manton_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0588_tgg_thaker_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0589_duke_ntd_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0591_workspace_for_familty2.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0592_tgg_thaker_geco_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0593_ntd_wes_2021dec.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0594_rare_genomes_project_gen.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0598_shc_oi.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0599_estonian_external_jan202.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0604_charge_seqr_analysis.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0607_gregor_training_project_.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0608_gregor_training_project_.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0620_gregor_southampton_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0621_gatk4_germline_preproces.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0622_gatk4_germline_preproces.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0623_neurodev_wave1_sa.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0624_neurodev_wave1_kenya.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0626_tgg_minikel_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0628_shc_scoliosis.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0630_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0631_ogi131_wgs_really_jointg.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0632_clinical.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0633_220121_cm_kb_rs_wgs_nova.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0635_gregor_estonia_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0636_mgh_pathways_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0637_cmt2.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0638_tgg_walsh_rgp_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0639_tgg_bonnemann_turkey_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0640_tgg_shimamura_sankaran_w.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0641_uw_acc2.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0642_exresgermline.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0643_meei_janeywiggs_germline.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0644_gregor_beggs_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0646_seqr.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0649_tgg_shimamura_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0650_uw.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0652_pipeline_test.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0655_seqr.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0657_global_at_family_datapla.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0666_analytixin_pilot_seqr_co.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0667_alsafrica_net_wits.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0668_non_als_controls.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0669_trio_seqr.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0670_alirezahaghighi_bwh_card.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0671_describe_wgs_seqr.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0672_describe_wes_seqr.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0673_alsafrica_net_uct.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0675_other_neurological_disor.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0676_icgnmd.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0677_meei_janeywiggs_germline.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0678_crmrnd_team.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0679_tgg_bgm.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0680_gregor_chiocciolishapiro.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0681_yale_202301.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0682_gregor_beggs_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0684_gregor_ogrady_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0686_seqr_demo.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0687_meei_janeywiggs_germline.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0688_sear_training.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0691_ni_honours_genomics_prac.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0692_gregor_muntoni_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0693_gmkf_gleeson_neuraltubes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0695_gmkf_gleeson_recessive_w.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0696_tgg_bgm_ext_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0698_seqr_reu.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0699_seqr_han_inherited_eye_d.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0700_tgg_bonnemann_turkey_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0702_gupta_exome_march2023_co.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0703_gentac_bav.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0705_gregor_schn_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0710_seqr_ogi131.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0711_seqr_jinuhan_inherited_e.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0712_mgb_gusella_hd2.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0713_gp2_seqr.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0714_validacao_cartucho.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0715_seqr_highmyopia_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0716_seqr_xt_study_southkorea.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0719_21s_seqr.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0722_abrams_wes_seqr.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0723_abrams_su_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0724_seqr_marianna_inherited_.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0727_icgd_bahrain_genome_prog.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0728_seqr_south_korea_inherit.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0729_gregor_fleming_wes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0731_seqr_reu_wes_2022.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0732_deneme.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0733_deneme2.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0735_deneme3.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0739_cmg_walsh_mni_pmg_exomes.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0740_russia_inherited_retinal.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0742_gss_anvil_seqr_test.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0743_wgs_trio_analysis.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0744_mgh_pathways_teaching.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0752_wgs_trio_seqr_analysis.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0756_ogi_wgs_try.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0758_master_workspace.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0761_icgd_collaboration_raj_g.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0765_cgap_fam1.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0766_cgap_fam2.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0768_genysis.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0770_ff_solo_wgs.ht'
    'gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0772_ogi_korea_wgs.ht',
]


for project_table_path in projects:
    assert dataset_type.value in project_table_path
    assert reference_genome.value in project_table_path
    ht = hl.read_table(project_table_path)
    if hasattr(ht, 'family_entries'):
        continue
    sample_type = hl.eval(ht.globals.sample_type)

    project_name = project_table_path.split('/')[-1].replace('.ht', '')
    try:
        pedigree_ht = import_pedigree(
            f'gs://seqr-datasets/v02/{reference_genome.value}/RDG_{sample_type}_Broad_Internal/base/projects/{project_name}/{project_name}_pedigree.tsv',
        )
    except Exception:
        pedigree_ht = import_pedigree(f'gs://seqr-datasets/v02/{reference_genome.value}/AnVIL_{sample_type}/{project_name}/base/{project_name}_pedigree.tsv')

    families = parse_pedigree_ht_to_families(pedigree_ht)
    sample_id_to_family_guid = hl.dict(
        {s: f.family_guid for f in families for s in f.samples},
    )
    ht = deglobalize_sample_ids(ht)
    ht = ht.select(
        filters=ht.filters,
        family_entries=hl.sorted(
            ht.entries.map(
                lambda x: (
                    x.annotate(
                        family_guid=sample_id_to_family_guid[x.s],
                    )
                ),
            )
            .group_by(lambda x: x.family_guid)
            .values()
            .map(
                lambda x: hl.sorted(x, key=lambda x: x.s),
            ),
            lambda fe: fe[0].family_guid,
        ),
    )
    ht = globalize_ids(ht)
    ht = ht.annotate(
        family_entries=(
            ht.family_entries.map(
                lambda fe: hl.or_missing(
                    fe.any(dataset_type.family_entries_filter_fn),
                    fe,
                ),
            )
        ),
    )
    ht.filter(ht.family_entries.any(lambda fe: ~hl.is_missing(fe)))

