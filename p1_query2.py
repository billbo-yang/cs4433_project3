import math
from pyspark import SparkContext, SparkConf, sql
from operator import add

MAX_SIZE = 10000
GRID_SIZE = 100
COVID_DIST = 6
ROWS = MAX_SIZE / GRID_SIZE


def key_locations(x, y):
    key0 = int(ROWS * math.floor(y / GRID_SIZE) + math.floor(x / GRID_SIZE))
    key1 = int(ROWS * math.floor((y - COVID_DIST) / GRID_SIZE) +
               math.floor((x + COVID_DIST) / GRID_SIZE))
    key2 = int(ROWS * math.floor((y + COVID_DIST) / GRID_SIZE) +
               math.floor((x + COVID_DIST) / GRID_SIZE))
    key3 = int(ROWS * math.floor((y + COVID_DIST) / GRID_SIZE) +
               math.floor((x - COVID_DIST) / GRID_SIZE))
    key4 = int(ROWS * math.floor((y - COVID_DIST) / GRID_SIZE) +
               math.floor((x - COVID_DIST) / GRID_SIZE))
    return set({key0, key1, key2, key3, key4})


def map_points(isInfected, line):
    output = []

    point_attributes = line.split(",")
    point_x = int(point_attributes[1])
    point_y = int(point_attributes[2])
    infected_id = -1
    if isInfected:
        infected_id = int(point_attributes[0])

    for key in key_locations(point_x, point_y):
        row = "{},{},{},{}".format(key, point_x, point_y, infected_id)
        output.append(row)

    return output


def reduce_infected(line):
    # print("A")
    # print(key)
    grid = line[0]
    people = line[1]
    infected_list = []
    person_list = []
    pairs = []
    for person in people:
        if person[2] != -1:
            infected_list.append(person[0:3])
        else:
            person_list.append(person[0:2])
    for infected in infected_list:
        infected_people = []
        for person in person_list:
            if math.sqrt((infected[0] - person[0])**2 + (infected[1] - person[1])**2) <= 6:
                infected_people.append(person)
        if len(infected_people) > 0:
            pairs.append((infected[2], infected_people))

    return pairs


# Creates Spark context.
sc = SparkContext("local", "PySpark Problem 1 Query 2")

# Reads the data from each data file.
point_lines = sc.textFile(
    "PEOPLE.csv").flatMap(lambda line: line.split("\n"))
infected_lines = sc.textFile(
    "INFECTED-large.csv").flatMap(lambda line: line.split("\n"))

# Parses and cleans the data of the people data points.
point_map = point_lines.map(
    lambda line: map_points(False, line)
).flatMap(
    lambda lines: lines
).map(
    lambda line: (int(line.split(",")[0]), [
                  (int(line.split(",")[1]), int(line.split(",")[2]), int(line.split(",")[3]))])
)

# Parses and cleans the data of the infected people data points and stores the
# infected id.
infected_map = infected_lines.map(
    lambda line: map_points(True, line)
).flatMap(
    lambda lines: lines
).map(
    lambda line: (int(line.split(",")[0]), [
                  (int(line.split(",")[1]), int(line.split(",")[2]), line.split(",")[3])])
)

# Combines the people and the infected people.
all_people = sc.union([point_map, infected_map])

# Use a map function to create the lists of people that 
# are near an infected person. Uses the folded lists of all people 
# that are in that grid cell.
all_pairs = all_people.foldByKey([], add).map(
    reduce_infected).flatMap(lambda lines: lines)

# Folds the lists of people by key of infected person.
# Removes duplicates from the pairs. Counts the number 
# of unique people affected by COVID case.
final_pairs = all_pairs.foldByKey([], add).map(
    lambda pairs: (pairs[0], len(set(pairs[1]))))

final_pairs.saveAsTextFile("temp2/")
