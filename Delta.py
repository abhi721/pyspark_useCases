"""
   Poc on Delta Lake Funtionlity on Cloudera
    1.Batch CRUD (Insert Update Delete ) operations on table
    2.Write to table
    3.Append
    4.Overwrite
    5.Schema Enforcement
    6.Update Table Schema
    7.Describe Lineage of table
"""
from deltalake import DeltaTable

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta import *

read_format = 'csv'
write_format = 'delta'
load_path = '/user/root/Bike_share.csv'
save_path = '/user/root/result'
table_name = 'test_delta.bike_share'


spark = SparkSession.builder.appName("delta_table").master("local").enableHiveSupport().getOrCreate()


bike_share_loaded = spark.read.format(read_format).option("inferSchema", "true").option("header", "true").load(load_path)

bike_share_data = bike_share_loaded.withColumnRenamed("Total duration (ms)","Total_duration").withColumnRenamed("Start date","Start_date").withColumnRenamed("Start station","Start_station").withColumnRenamed("End date","End_date").withColumnRenamed("End station","End_station").withColumnRenamed("Bike number","Bike_number").withColumnRenamed("Subscription Type","Subscription_Type")

"""
Insert data into Delta format
"""

bike_share_data.write.format(write_format).save(save_path)

"""
Delta does not have the SQL Create Table syntax in Spark 2.4 version. This is implemented in Spark 3.0. 
For CRUD operations,we must use DeltaTable.forPath below spark 3.0.
Using spark 3.0,

Updating deltaTable as following
"""
deltaTable = DeltaTable.forPath(spark, "/user/root/result")
deltaTable.update('Total_duration =2394764',{'Start_station':"'BKSC'"})

"""
Deleting same updated record i.e Total_duration =2394764
"""
deltaTable.delete('Total_duration =2394764')

"""
Appending and Schema Enforcment 
"""

#/user/root/result'  19  7 column    38
#new column = 19 rows 6 c
df1 = spark.read.format('delta').load('/user/root/result')
df2 =df1.drop('Bike_number')
df2.write.format("delta").mode("append").save("/user/root/result")

# 19 rows

#after append 10


#final ==29



