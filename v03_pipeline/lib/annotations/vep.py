def vep(
    ht: hl.Table,
    env: Env,
    reference_genome: ReferenceGenome,
    vep_config_json_path: str,
    **kwargs: Any,
) -> hl.Table:
    if hasattr(ht, 'vep'):
        return ht
    vep_runner = (
        vep_runners.HailVEPRunner()
        if env != Env.TEST
        else vep_runners.HailVEPDummyRunner()
    )
    return vep_runner.run(
        ht,
        reference_genome.v02_value,
        vep_config_json_path=vep_config_json_path,
    )
