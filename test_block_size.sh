#IP_ADDRESS=10.44.0.7
#IP_ADDRESS=10.48.3.5
IP_ADDRESS=10.16.1.5

#for i in 50 100 500 1000 5000 10000 50000 100000; do
for i in 50 100 1000; do 
    echo =========================
    echo Testing with $i samples 
    echo =========================
    date
    ./submit.py --project=seqr-project --cluster=no-vep2 export_variants_to_ES.py -H $IP_ADDRESS --port 9200 --index engle_wgs_900_samples_all__chr22_subset_non_coding_ssd__200_samples_block_size_${i} gs://seqr-datasets/GRCh38/20170629_900Genomes_full_239969348564/900Genomes_full.chr22_subset.vep.non_coding.all_annotations.vds --block-size $i --num-samples 200
    date
done
