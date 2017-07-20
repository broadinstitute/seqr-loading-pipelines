#!/bin/bash
if [ $# -lt 1 ]; then
    #echo 'usage: gcp-pyhail-submit <cluster> <py-files>'
    echo 'usage: pyhail-submit <py-file>'
    exit 1
fi

#CLUSTER=$1
#shift
CLUSTER=seqr-${USER}-cluster
SCRIPT=$1
shift
echo cluster = $CLUSTER
echo script = $SCRIPT
HASH=5fc383e  #`gsutil cat gs://hail-common/latest-hash.txt`
JAR_FILE=hail-hail-is-master-all-spark2.0.2-$HASH.jar
JAR=gs://hail-common/$JAR_FILE
PYFILES=gs://hail-common/pyhail-hail-is-master-$HASH.zip
for file in "$@"
do
    PYFILES="$PYFILES,$file"
done

echo JAR = $JAR
echo pyfiles = $PYFILES
gcloud dataproc --project seqr-project \
    jobs submit pyspark \
       --cluster $CLUSTER \
       --files=$JAR \
       --py-files=$PYFILES \
       --properties="spark.driver.extraClassPath=./$JAR_FILE,spark.executor.extraClassPath=./$JAR_FILE" \
       $SCRIPT \
