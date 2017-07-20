echo num-workers: $1
echo num-preemptible-workers: $2

gcloud dataproc clusters --project seqr-project update --num-workers $1 --num-preemptible-workers $2  seqr-${USER}-cluster
