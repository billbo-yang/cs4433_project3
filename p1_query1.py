import math

from pyspark import SparkContext, SparkConf

def map_points(infected_list, line):
    output = ""

    point_attributes = line.split(",")
    point_x = int(point_attributes[1])
    point_y = int(point_attributes[2])

    for infected_attribute in infected_list:
        infected_id = int(infected_attribute[0])
        infected_x = int(infected_attribute[1])
        infected_y = int(infected_attribute[2])
        dist = math.sqrt((infected_x - point_x)**2 + (infected_y - point_y)**2)
        if dist <= 6:
            if not output:
                output = "{}".format(infected_id)
            else:
                output = output + "\n{}".format(infected_id)
    if output:
        return output

def load_infected_file(filename):
    infected_file = open(filename, "r")
    infected_lines = infected_file.read()
    infected_file.close()

    infected_lines_list = infected_lines.splitlines()
    infected_list = []
    for line in infected_lines_list:
        infected_list.append(line.split(","))
    
    return infected_list

# create Spark context with necessary configuration
sc = SparkContext("local", "PySpark Word Count Exmaple")

# read infected points from file and store in memory
infected_list = load_infected_file("INFECTED-small.csv")

# read data from text file and split each line into its normal point attributes
point_lines = sc.textFile("PEOPLE.csv").flatMap(lambda line: line.split("\n"))

# for each normal point, check to see if its in range of any infected point
point_map = point_lines.map(
        lambda line: map_points(infected_list, line)
).filter(
    lambda line: line
).map(
    lambda infected_id: (infected_id, 1)
)

# count the number of occurrances of each infected id to get number of close contacts
close_count = point_map.reduceByKey(lambda x,y:(x+y))

# save the counts to output
close_count.saveAsTextFile("temp/")
