# k-means: Map-Reduce with spark

This repository is based on [seraogianluca/k-means-mapreduce](https://github.com/seraogianluca/k-means-mapreduce) and rewritten to work with stand-alone Spark (without Hadoop). The original Readme, describing and showing the k-means algorithm was moved to [doc/README.md](https://github.com/hu-macsy/k-means-mapreduce/blob/master/doc/README.md).

## Running spark on a local system (single process)

1. Download the latest [spark-standalone archive](https://www.apache.org/dyn/closer.lua/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz) and extract the contents. Either add the `bin`-folder from the archive to your path or call `spark-submit` with the complete path.

2. Clone repo and cd into repo:

```
git clone https://github.com/hu-macsy/k-means-mapreduce/
cd k-means-mapreduce
```

3. Execute `spark-submit`:

```
spark-submit --total-executor-cores 2 --executor-memory 2G ./k-means-spark/spark.py ./datasets/10k/dataset_3_13.txt ./output/test.out
```

While the explicit setup is done in `spark.py`, `spark-submit` handles the creation of the necessary computation environment. It launches `k-means-spark/spark.py` on a medium sized dataset (`datasets/10k/dataset_3_13.txt`). Based on how you decide to solve the task, changes may be necessary (both for script and dataset). You can see the log of the computation directly on stdout, while the output is written to `output`-folder.

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

Both log and output from the job is written to `output`-folder. The file-names (both log and output) contain the job-id, visible after job-submission and via `squeue`.

### Basic: Changing configuration of slurm-script for your task

In the default state `k-means-slurm-spark.sh` configures all spark related variables and launches `k-means-spark/spark.py` on a medium sized dataset (`datasets/10k/dataset_3_13.txt`). Based on how you decide to solve the task, changes may be necessary (both for script and dataset). Be sure not to modify the environment variables in the script, this may break the functionality. 

Also take a look at how `spark.py` implements and creates a SparkSession. Be sure to match this kind of initialisation if you want to run a different script with slurm. Background: PySpark supports several initialisation mechanisms. The current implementation of `spark.py` uses a local `SparkSession`, relying on spark-submit + slurm to handle ressources. Other Session types (like `yarn`, `hadoop`) need hadoop as back-end infrastructure. This is not available on the HU department of Computer Science infrastructure.

### Advanced: Optimization of ressource allocation

This is optional for the task. You can leave all related variables as they are. However, if you think this can improve running time - you can modify the ressource usage (line 5-9 + line 51).

Number of CPU-cores/nodes:

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
spark-submit --total-executor-cores 16 --executor-memory 2G ${REPO_PATH}/k-means-spark/spark.py file://${REPO_PATH}/datasets/10k/dataset_3_13.txt ${REPO_PATH}/output/output_${SLURM_JOB_ID}.out
```

Line 5-9 set the ressource allocation for slurm. Four worker nodes are allocated, where on each node two worker processes are spawned. Each of these processes may use up to two cores. In total 16 CPU-cores are available for computation. The amount of Spark workers is set in line 51 by parameter `--total-executor-cores`. Note, that this number should match the number of `#nodes * #ntask-per-node * #cpus-per-task`. The script also works if `--total-executor-cores` is set to a different number, but in general this leads to lower performance and over/underprovisioning. Feel free to change ressource allocations, but be aware that communcation penalty increases for higher number of Spark workers.

Amount of memory:

```
(line 10-11)
# Set memory allocation per cpu
#SBATCH --mem-per-cpu 1G
```

```
(line 51)
spark-submit --total-executor-cores 16 --executor-memory 2G ${REPO_PATH}/k-means-spark/spark.py file://${REPO_PATH}/datasets/10k/dataset_3_13.txt ${REPO_PATH}/output/output_${SLURM_JOB_ID}.out
```

Line 11 again set the amount of memory, which slurm allocates per CPU-core. Since per default `--cpus-per-task` is set to two, each worker task can use up to `2G` of memory. This amount is set again in line 51 by parameter `--executor-memory`. Again, this value can be adjusted accordingly. However be sure to match `#cpus-per-task * #mem-per-cpu = #executor-memory`, otherwise the job may fail or waste ressources. 