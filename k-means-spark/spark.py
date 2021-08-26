import sys
import os
import json
from numpy import add
from pyspark import SparkContext
from pyspark.sql import SparkSession
from point import Point
from operator import add
import time

def init_centroids(dataset, k):
    """
    Randomly select `k` points from the loaded `dataset` as initial centroits
    Benchmarked with time
    """
    start_time = time.time()
    initial_centroids = dataset.takeSample(False, k)
    print("init centroid execution:", len(initial_centroids), "in", (time.time() - start_time), "s")
    return initial_centroids

def assign_centroids(p):
    """
    Returns the nearest centroid of p, as well as p itself

    The centroids are taken from the centroids_broadcast
    """
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
    """
    When centroids haven't changed more than `threshold` in distance, the centroids have been found
    """
    old_centroids = centroids_broadcast.value
    for i in range(len(old_centroids)):
        check = old_centroids[i].distance(new_centroids[i], distance_broadcast.value) <= threshold
        if check == False:
            return False
    return True

def join_centroids(p):
    centroids = centroids_broadcast.value
    list = []
    for i in range(len(centroids)):
        centroid = centroids[i]
        list.append((p, centroid))
    return list

def group_distances(distance_rdd, f):
    get_distance = lambda x: [value[1] for value in list(x)]
    sum_distance = lambda x: sum(x)
    distance_by_points_rdd = distance_rdd.groupBy(f).mapValues(get_distance)
    total_distance_by_points_rdd = distance_by_points_rdd.mapValues(sum_distance)
    return (distance_by_points_rdd, total_distance_by_points_rdd)

def get_distances(distance_rdd):
    (distance_by_points_rdd, total_distance_by_points_rdd) = group_distances(distance_rdd, lambda x: x[0][0])
    (distance_by_centroids_rdd, total_distance_by_centroids_rdd) = group_distances(distance_rdd, lambda x: x[0][1])
    return (distance_by_points_rdd, distance_by_centroids_rdd, total_distance_by_points_rdd, total_distance_by_centroids_rdd)

def divide_distance(wtf):
    return (wtf[1][0][0], 1-wtf[1][0][1]/wtf[1][1])

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

    joined_point_centroids_rdd = points.map(join_centroids).flatMap(lambda x: x)

    distance_rdd = joined_point_centroids_rdd.map(lambda a: (a, a[0].distance(a[1], distance_broadcast.value)))
    (distance_by_points_rdd, distance_by_centroids_rdd, total_distance_by_points_rdd, total_distance_by_centroids_rdd) = get_distances(distance_rdd)

    foo = distance_rdd.map(lambda x: (x[0][1], x)).join(total_distance_by_centroids_rdd).map(divide_distance)
    (distance_by_points_rdd, distance_by_centroids_rdd, total_distance_by_points_rdd, total_distance_by_centroids_rdd) = get_distances(foo)
    # print(total_distance_by_points_rdd.collect())

    execution_time = time.time() - start_time
    print("\nexecution time:", execution_time, "s")
    # print("average time per iteration:", execution_time/n, "s")
    # print("n_iter:", n)
