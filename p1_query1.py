import sys

from pyspark import SparkContext, SparkConf

def map_points(point_attributes):
    id = point_attributes[0]
    x_coord = point_attributes[1]
    y_coord = point_attributes[2]
    


# create Spark context with necessary configuration
sc = SparkContext("local", "PySpark Word Count Exmaple")

# read infected points from file and store in memory


# read data from text file and split each line into its normal point attributes
points = sc.textFile("PEOPLE.csv").flatMap(lambda line: line.split(","))

# for each normal point, check to see if its in range of any infected point
point_map = points.map(
        lambda attributes: map_points(attributes)
    )

# save the counts to output
# wordCounts.saveAsTextFile("temp/")
