#IP_ADDRESS=10.44.0.7
#IP_ADDRESS=10.48.3.5
#IP_ADDRESS=10.28.4.4

#IP_ADDRESS=10.48.0.5
IP_ADDRESS=10.48.5.5

cd ..

#for i in 3 50 100 200 300 500 700 900; do
#for i in 200 225 250 275 300; do
for i in 200 250 300 330; do
    echo =========================
    echo Testing with $i samples 
    echo =========================
    date
    ./submit.py --project=seqr-project --cluster=no-vep export_variants_to_ES.py -H $IP_ADDRESS --port 9200 --index engle_wgs_900_samples_all__chr22_subset_non_coding_ssd__${i}_samples gs://seqr-datasets/GRCh38/20170629_900Genomes_full_239969348564/900Genomes_full.chr22_subset.vep.non_coding.all_annotations.vds --block-size 50 --num-samples $i
    date
done
