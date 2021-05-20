#!/bin/bash

#SBATCH --job-name=spark-test
# Number of Nodes
#SBATCH --nodes=4
# Number of processes per Node
#SBATCH --ntasks-per-node=2
# Number of CPU-cores per task
#SBATCH --cpus-per-task=2
# Disable Hyperthreads
#SBATCH --ntasks-per-core=1

export SPARK_HOME=/glusterfs/pool-gfs-dist/spark/spark-3.1.1-bin-hadoop3.2

VERSION=spark-3.1.1-bin-hadoop3.2
DIRECTORY=/glusterfs/pool-gfs-dist/spark

export PATH=$PATH:${DIRECTORY}/${VERSION}/sbin
export PATH=$PATH:${DIRECTORY}/${VERSION}/bin
export PATH=$PATH:${DIRECTORY}/spark-common/bin

export SPARK_LOG_DIR=/tmp
export SPARK_WORKER_DIR=/tmp
export SPARK_LOCAL_DIRS=/tmp
export SPARK_CONF_DIR=~/.spark-config

echo $DIRECTORY
echo $PATH

spark-start

echo $MASTER

echo ""
echo " About to run the spark job"
echo ""

spark-submit --total-executor-cores 16 --executor-memory 1G ${DIRECTORY}/spark-common/examples/kmeans/k-means-spark/spark.py file://${DIRECTORY}/spark-common/examples/kmeans/datasets/100k/dataset_3_13.txt ${DIRECTORY}/../brandtfa/100k_3_13.output