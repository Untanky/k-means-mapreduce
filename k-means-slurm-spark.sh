#!/bin/bash

#SBATCH --job-name=spark-test
# Number of Nodes
#SBATCH --nodes=4
# Number of processes per Node
#SBATCH --ntasks-per-node=2
# Number of CPU-cores per task
#SBATCH --cpus-per-task=2
# Set memory allocation per cpu
#SBATCH --mem-per-cpu 1G
# Disable Hyperthreads
#SBATCH --ntasks-per-core=1
# Set output folder
#SBATCH -o ./output/log_%j.out

export SPARK_HOME=/glusterfs/pool-gfs-dist/spark/spark-3.1.1-bin-hadoop3.2

VERSION=spark-3.1.1-bin-hadoop3.2
DIRECTORY=/glusterfs/pool-gfs-dist/spark

#Credit: https://stackoverflow.com/questions/56962129/how-to-get-original-location-of-script-used-for-slurm-job
if [ -n $SLURM_JOB_ID ];  then
    # check the original location through scontrol and $SLURM_JOB_ID
    SCRIPT_PATH=$(scontrol show job $SLURM_JOBID | awk -F= '/Command=/{print $2}')
else
    # otherwise: started with bash. Get the real location.
    SCRIPT_PATH=$(realpath $0)
fi

REPO_PATH=$(dirname "${SCRIPT_PATH}")

echo $REPO_PATH

export PATH=$PATH:${DIRECTORY}/${VERSION}/sbin
export PATH=$PATH:${DIRECTORY}/${VERSION}/bin
export PATH=$PATH:${DIRECTORY}/spark-common/bin

export SPARK_LOG_DIR=/tmp
export SPARK_WORKER_DIR=/tmp
export SPARK_LOCAL_DIRS=/tmp
export SPARK_CONF_DIR=~/.spark-config


spark-start

echo $MASTER

echo ""
echo " About to run the spark job"
echo ""

spark-submit --total-executor-cores 16 --executor-memory 2G ${REPO_PATH}/k-means-spark/spark.py file://${REPO_PATH}/datasets/10k/dataset_3_13.txt ${REPO_PATH}/output/output_${SLURM_JOB_ID}.out
