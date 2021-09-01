import sys
import os
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from point import Point
import time

def init_centroids(dataset, k):
    start_time = time.time()
    initial_centroids = dataset.takeSample(False, k)
    print("init centroid execution:", len(initial_centroids), "in", (time.time() - start_time), "s")
    return initial_centroids

def setup_graph(points, k, distance):
    centroids = centroids_broadcast.value
    slots = sc.parallelize(range(0, points.count()))
    centroid_by_slot = slots.map(lambda slot_id: (slot_id, centroids[slot_id % k]))
    graph = points.cartesian(centroid_by_slot).map(lambda x: ((x[0], x[1][0]), x[0].distance(x[1][1], distance)))
    return graph

def get_matching(graph):
    distance = lambda edge: edge[1]
    matching = []
    points = {}
    slots = {}
    graph = graph.sortBy(distance).keys().collect()
    
    for edge in graph:
        point = edge[0]
        slot = edge[1]
        if not(str(hash(point)) in points or str(hash(slot)) in slots):
            matching.append(edge)
            points[str(hash(point))] = 1
            slots[str(hash(slot))] = 1
    
    print(len(matching))
    return matching

def assign_centroids(p):
    min_dist = float("inf")
    centroids = centroids_broadcast.value
    nearest_centroid = 0
    for i in range(len(centroids)):
        distance = p.distance(centroids[i], distance_broadcast.value)
        if(distance < min_dist):
            min_dist = distance
            nearest_centroid = i
    return (nearest_centroid, p)

def stopping_criterion(new_centroids, threshold):
    old_centroids = centroids_broadcast.value
    for i in range(len(old_centroids)):
        check = old_centroids[i].distance(new_centroids[i], distance_broadcast.value) <= threshold
        if check == False:
            return False
    return True

if __name__ == "__main__":
    start_time = time.time()
    if len(sys.argv) != 3:
        print("Number of arguments not valid!")
        sys.exit(1)

    with open(os.path.join(os.path.dirname(__file__),"./config.json")) as config:
        parameters = json.load(config)["configuration"][0]

    INPUT_PATH = str(sys.argv[1])
    OUTPUT_PATH = str(sys.argv[2])
    
    spark = SparkSession.builder.appName("PySparkkmeans").getOrCreate()
    sc = spark.sparkContext
    #sc = SparkContext("yarn", "Kmeans")
    sc.setLogLevel("ERROR")
    sc.addPyFile(os.path.join(os.path.dirname(__file__),"./point.py")) ## It's necessary, otherwise the spark framework doesn't see point.py

    print("\n***START****\n")

    points = sc.textFile(INPUT_PATH).map(Point).cache()
    initial_centroids = init_centroids(points, k=parameters["k"])
    distance_broadcast = sc.broadcast(parameters["distance"])
    centroids_broadcast = sc.broadcast(initial_centroids)

    stop, n = False, 0
    while True:
        print("--Iteration n. {itr:d}".format(itr=n+1), end="\r", flush=True)

        graph = setup_graph(points, parameters["k"], parameters["distance"])
        matching = get_matching(graph)

        cluster_assignment_rdd = sc.parallelize(matching).map(lambda x: (x[1] % parameters["k"], x[0]))
        sum_rdd = cluster_assignment_rdd.reduceByKey(lambda x, y: x.sum(y))
        centroids_rdd = sum_rdd.mapValues(lambda x: x.get_average_point()).sortByKey()

        new_centroids = centroids_rdd.values().collect()
        stop = stopping_criterion(new_centroids,parameters["threshold"])

        n += 1
        if(stop == False and n < parameters["maxiteration"]):
            centroids_broadcast = sc.broadcast(new_centroids)
        else:
            break

    with open(OUTPUT_PATH, "w") as f:
        for centroid in new_centroids:
            f.write(str(centroid) + "\n")

    execution_time = time.time() - start_time
    print("\nexecution time:", execution_time, "s")
    print("average time per iteration:", execution_time/n, "s")
    print("n_iter:", n)
