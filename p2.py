import math
from pyspark import SparkContext, SparkConf, sql
from pyspark.sql.types import *
import pyspark.sql.functions as func

# Creates Spark context.
sc = SparkContext("local", "PySpark Problem 1 Query 2")
spark = sql.SparkSession(sc)

customers_schema = StructType([ \
    StructField("id",IntegerType(),True), \
    StructField("name",StringType(), True), \
    StructField("age",IntegerType(), True), \
    StructField("country_code",IntegerType(), True), \
    StructField("salary",FloatType(), True) \
])

purchases_schema = StructType([ \
    StructField("trans_id", IntegerType(), True), \
    StructField("cust_id", IntegerType(), True), \
    StructField("trans_total", FloatType(), True), \
    StructField("trans_num_items", IntegerType(), True), \
    StructField("trans_desc", StringType(), True), \
])

# load data into dataframes
customers_df = spark.read.csv("CUSTOMERS.csv", schema=customers_schema)
purchases_df = spark.read.csv("PURCHASES.csv", schema=purchases_schema)

# Task 1
print("~~~ Task 1 ~~~")
purchases_df_t1 = purchases_df.filter(purchases_df.trans_total <= 600)
purchases_df_t1.show()

# Task 2
print("~~~ Task 2 ~~~")
purchases_df_t2_max = purchases_df_t1.groupBy('trans_num_items').agg({'trans_total': 'max'})
purchases_df_t2_min = purchases_df_t1.groupBy('trans_num_items').agg({'trans_total': 'min'})
purchases_df_t2_median = purchases_df_t1.groupBy('trans_num_items').agg(func.percentile_approx('trans_total', 0.5).alias('median(trans_total)'))
print("t2_max")
purchases_df_t2_max.show()
print("t2_min")
purchases_df_t2_min.show()
print("t2_median")
purchases_df_t2_median.show()

# Task 3
print("~~~ Task 3 ~~~")
# We believe we have already completed this with our print statements from part 2
print("See Task 2 :)")

# Task 4
print("~~~ Task 4 ~~~")
# df.join(df2, df.name == df2.name, 'outer')
joined_df_t4 = purchases_df_t1.join(customers_df, customers_df.id == purchases_df_t1.cust_id, 'inner')
joined_df_t4 = joined_df_t4.filter(joined_df_t4.age <= 25).filter(joined_df_t4.age >= 18)
grouped_df_t4 = joined_df_t4.groupBy('cust_id', 'age').sum('trans_num_items', 'trans_total')
print("t4_joined")
joined_df_t4.show()
print("t4_grouped")
grouped_df_t4.show()

# Task 5
print("~~~ Task 5 ~~~")
renamed_df_1 = grouped_df_t4.withColumnRenamed('sum(trans_num_items)', 'trans_num_items_sum') \
                            .withColumnRenamed('sum(trans_total)', 'trans_total_sum')

renamed_df_2 = grouped_df_t4.withColumnRenamed('cust_id', 'cust_id_2') \
            .withColumnRenamed('age', 'age_2') \
            .withColumnRenamed('sum(trans_num_items)', 'trans_num_items_sum_2') \
            .withColumnRenamed('sum(trans_total)', 'trans_total_sum_2')

crossjoined_df_t5 = renamed_df_1.crossJoin(renamed_df_2)
crossjoined_df_t5 = crossjoined_df_t5.filter(crossjoined_df_t5.age < crossjoined_df_t5.age_2) \
    .filter(crossjoined_df_t5.trans_num_items_sum > crossjoined_df_t5.trans_num_items_sum_2)
print("~~~ customer pairs where c1.age < c2.age w/ other filters ~~~")
crossjoined_df_t5.select('cust_id', 'cust_id_2').show()

# Task 6
print("~~~ Task 6 ~~~")
df_t6 = crossjoined_df_t5.select('cust_id', 'cust_id_2', 'age', 'age_2',
                                'trans_num_items_sum', 'trans_num_items_sum_2',
                                'trans_total_sum', 'trans_total_sum_2')

df_t6.printSchema()
df_t6.show()