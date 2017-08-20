for i in 3 50 100 200 300 500 700 900; do
    echo =========================
    echo Testing with $i samples 
    echo =========================
    date
    ./submit.py --project=seqr-project --cluster=no-vep export_variants_to_ES.py -H 10.44.0.7 --port 9200 --index engle_wgs_900_samples_all__chr22_subset_non_coding__${i}_samples gs://seqr-datasets/GRCh38/20170629_900Genomes_full_239969348564/900Genomes_full.chr22_subset.vep.non_coding.all_annotations.vds --block-size 50 --num-samples $i
    date
done