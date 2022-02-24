import math
from pyspark import SparkContext, SparkConf, sql

GRID_SIZE = 100
COVID_DIST = 6

def key_locations(x, y):
    key0 = math.floor(x / GRID_SIZE) * math.floor(y / GRID_SIZE) + math.floor(x / GRID_SIZE)
    key1 = math.floor((x + COVID_DIST) / GRID_SIZE) * math.floor((y + COVID_DIST) / GRID_SIZE) + math.floor((x + COVID_DIST) / GRID_SIZE)
    key2 = math.floor((x + COVID_DIST) / GRID_SIZE) * math.floor((y - COVID_DIST) / GRID_SIZE) + math.floor((x + COVID_DIST) / GRID_SIZE)
    key3 = math.floor((x - COVID_DIST) / GRID_SIZE) * math.floor((y + COVID_DIST) / GRID_SIZE) + math.floor((x - COVID_DIST) / GRID_SIZE)
    key4 = math.floor((x - COVID_DIST) / GRID_SIZE) * math.floor((y - COVID_DIST) / GRID_SIZE) + math.floor((x - COVID_DIST) / GRID_SIZE)
    return set({key0, key1, key2, key3, key4})

def map_points(isInfected, line):
    output = ""

    point_attributes = line.split(",")
    point_x = int(point_attributes[1])
    point_y = int(point_attributes[2])

    for key in key_locations(point_x, point_y):
        row = "{},{},{},{}".format(key, point_x, point_y, isInfected)
        if not output:
            output = row
        else:
            output = output + "\n" + row
    
    if output:
        return output

# create Spark context with necessary configuration
sc = SparkContext("local", "PySpark Problem 1 Query 2")
spark = sql.SparkSession(sc)

# read data from text file and split each line into its normal point attributes
point_lines = sc.textFile("PEOPLE-small.csv").flatMap(lambda line: line.split("\n"))
infected_lines = sc.textFile("INFECTED-large.csv").flatMap(lambda line: line.split("\n"))

# for each normal point, check to see if its in range of any infected point
point_map = point_lines.map(
        lambda line: map_points(False, line)
).map(
        lambda line: (int(line.split(",")[0]),int(line.split(",")[1]),int(line.split(",")[2]),line.split(",")[3])
)
infected_map = infected_lines.map(
        lambda line: map_points(True, line)
).map(
        lambda line: (int(line.split(",")[0]),int(line.split(",")[1]),int(line.split(",")[2]),line.split(",")[3])
)

people = spark.createDataFrame(point_map, schema=['grid', 'x', 'y','infected'])
infected_people = spark.createDataFrame(infected_map, schema=['grid', 'x', 'y','infected'])

all_people = people.join(infected_people, on='grid')

out = all_people.rdd
out.saveAsTextFile("temp2/")