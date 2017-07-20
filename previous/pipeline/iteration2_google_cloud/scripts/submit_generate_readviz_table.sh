# read -i gs://gnomad/gnom.ad.vds \
# read -i gs://gnomad/gnomad.10ksites.vds \

../hail_gcloud_submit.sh bw2-cluster \
read -i gs://gnomad/gnom.ad.vds \
annotatesamples table -i gs://gnomad/gnomad.final.all_meta.txt -e Sample --impute -r sa.all_meta \
annotatesamples table -i gs://gnomad/gnomad.final.meta.txt -e sample --impute -r sa.meta \
annotatesamples table -i gs://gnomad/gnomad_projects_metadata.txt -e SAMPLE_ALIAS --impute -r sa.project_meta \
filtervariants intervals -i gs://gnomad-hail-bucket/gencode_exons_plus_75bp.intervals.gz --keep \
filtervariants expr -c "gs.filter(g => g.isCalledNonRef && g.gq>19 && g.dp > 9).count()>0" --keep \
filtersamples expr --keep -c 'sa.all_meta.releasable == true' \
annotatesamples expr -c 'sa.ismale = sa.meta.sex == "male"' \
exportsamples -c 'sa' -o gs://gnomad-hail-bucket/readviz/sample_table.txt \
printschema \
count \
readviz --annotations '
bam=sa.project_meta.BAM, 
gvcf=sa.project_meta.ON_PREM_GVCF, 
project_id = sa.meta.project_or_cohort, 
project_description = sa.meta.project_description, 
sex = sa.meta.sex , 
population = sa.meta.population
' --isMale-annotation "sa.ismale" -o gs://gnomad-hail-bucket/readviz/gnomad.coding.txt.gz 