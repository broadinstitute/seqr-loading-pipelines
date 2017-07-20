hail-spark-lf read -i file:///mnt/lustre/lfran/gnomad/gnomad.coding.vep.vds \
annotatesamples table -i file:///mnt/lustre/konradk/exac/super_meta.txt.bgz -e sample --impute -r sa.meta \
filtersamples expr --keep -c 'sa.meta.drop_status == "keep"' \
filtervariants expr -c "gs.filter(g => g.isCalledNonRef && g.gq>19 && g.dp > 9).count()>0" --keep \
annotatesamples expr -c 'sa.ismale = sa.meta.sex == "male"' \
readviz --annotations 'bam=sa.meta.bam, gvcf=sa.meta.gvcf, project_id = sa.meta.pid, project_description = sa.meta.description, sex = sa.meta.sex , population = sa.meta.population' --isMale-annotation "sa.ismale" -o file:///mnt/lustre/weisburd/data/gnomad.coding.txt.gz
