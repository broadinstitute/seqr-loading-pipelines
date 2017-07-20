../hail_gcloud_submit.sh bw2-cluster \
read -i gs://gnomad/gnom.ad.vds \
annotatesamples table -i gs://gnomad/gnomad.final.all_meta.txt -e Sample --impute -r sa.all_meta \
annotatesamples table -i gs://gnomad/gnomad.final.meta.txt -e sample --impute -r sa.meta \
annotatesamples table -i gs://gnomad/gnomad_projects_metadata.txt -e SAMPLE_ALIAS --impute -r sa.project_meta \
filtervariants intervals -i gs://gnomad-hail-bucket/gencode_exons_plus_75bp.intervals.gz --keep \
filtervariants expr -c "gs.filter(g => g.isCalledNonRef && g.gq>19 && g.dp > 9).count()>0" --keep \
filtersamples expr --keep -c 'sa.project_meta.releasable == "YES"' \
annotatesamples expr -c 'sa.ismale = sa.meta.sex == "male"' \
printschema \
count 