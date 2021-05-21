# k-means: Map-Reduce with spark

This repository is based on [seraogianluca/k-means-mapreduce](https://github.com/seraogianluca/k-means-mapreduce) and rewritten to work with stand-alone Spark (without Hadoop). The original Readme, describing and showing the k-means algorithm was moved to [doc/README.md](https://github.com/hu-macsy/k-means-mapreduce/blob/master/doc/README.md).

## Running spark on a local system (single process)


## Running spark with slurm (multi process)

Besides the possibilty to setup a personal Spark cluster (could also be Hadoop), Spark supports being initialised via slurm (scheduling system). This repo provides a complete and portable script, ready-to-use with the slurm infrastructure at the HU department of Computer Science. For more background information on how slurm works, there is german [doc site](https://www.informatik.hu-berlin.de/de/org/rechnerbetriebsgruppe/dienste/hpc/slurm) available.

The following is a minimal example of how to run the code of this repo in the infrastructure. 

1. Login to any gruenauX:

```
ssh <USERNAME>@gruenauX
```

Note: This requires that you have remote access to the HU department of Computer Science server. Normally this is achieved by using either CSM or the department VPN-service.

2. Clone repo and cd into repo:

```
git clone https://github.com/hu-macsy/k-means-mapreduce/
cd k-means-mapreduce
```

3. Submit job to slurm via `sbatch`:

```
sbatch k-means-slurm-spark.sh
```

You can view the current status of your task by calling `squeue`:

```
brandtfa 54 ( k-means-mapreduce ) $ squeue
JOBID      PARTITION     NAME     USER      ST       TIME       NODES NODELIST(REASON)
67108957   defq          spark-te brandtfa  R        16:24      4     kudamm,lankwitz,marzahn,mitte
```

### Changing configuration of slurm-script

In the default state `k-means-slurm-spark.sh` configures all spark related variables and launches a specific file (`k-means-spark/spark.py`) on a medium sized dataset (`datasets/10k/dataset_3_13.txt`). Based on how you decide to solve the task, changes may be necessary. Be sure not to modify the environment variables in the script, this may break the functionality. You can modify the ressource usage (line 5-9, line 51).

```
(line 5-9)
# Number of Nodes
#SBATCH --nodes=4
# Number of processes per Node
#SBATCH --ntasks-per-node=2
# Number of CPU-cores per task
#SBATCH --cpus-per-task=2
```

```
(line 51)
spark-submit --total-executor-cores 16 --executor-memory 1G ${REPO_PATH}/k-means-spark/spark.py file://${REPO_PATH}/datasets/10k/dataset_3_13.txt ${REPO_PATH}/output/output_${SLURM_JOB_ID}.out
```

This sets the ressource allocation for slurm. Four worker nodes are allocated, where on each node two worker processes are spawned. Each of these processes may use up to two cores. In total 16 cores are available for computation. The amount of workers is set in line 51 by parameter `--total-executor-cores`. Note, that this number should match the number of `#nodes * #ntask-per-node * #cpus-per-task`. The script also works if `--total-executor-cores` is set to a different parameter, but this leads to lower performance and over/underprovisioning. Feel free to change ressource allocations, but be aware that communcation penalty increases for higher number of Spark workers.

### Changing initialisation of spark

PySpark supports several initialisation mechanism. The current implementation of `k-means-spark/spark.py` uses a local `SparkSession`, relying on spark-submit + slurm to handle ressources. Other Session types (like `yarn`, `hadoop`) need hadoop as back-end infrastructure. This is not available on the HU department of Computer Science infrastructure.