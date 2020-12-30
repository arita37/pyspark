#!/usr/bin/env python
# coding: utf-8

# In[149]:


from pyspark import SparkContext
from pyspark import SparkConf,StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import dense_rank,rank,row_number,max,sum,broadcast,col,lit,collect_set,explode,flatten
from pyspark.sql.functions import approx_count_distinct,collect_list
from pyspark.sql.functions import collect_set,sum,avg,max,countDistinct,count
from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness 
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct
from pyspark.sql.functions import variance,var_samp,  var_pop
from pyspark.sql.window import Window
#import boto3
import os, sys


os.chdir("exo/")
os.getcwd()
root  = os.getcwd() + "/"





spark = SparkSession.builder.master("local").appName("PySpark").getOrCreate()




path = root + "test-table/"




df = spark.read.json(path)


df.printSchema()

df.createGlobalTempView("data")




df.show(3)


# ## 1. Cache & Persist
## Here cache and persist both works same way , 
## but persist allows you to choose storage of your choice.

df.cache()
df.cache().explain(1000)



df.persist(StorageLevel.MEMORY_ONLY)
df.persist(StorageLevel.MEMORY_ONLY_SER)
df.persist(StorageLevel.MEMORY_AND_DISK)  ### disk and dis
df.persist(StorageLevel.DISK_ONLY)


# ## 2. Unpersist or remove all cached dataframes
spark.catalog.clearCache()

spark.catalog.dropGlobalTempView("data")  ## specific view

df.unpersist()



# ## 3. Types of joins
df1 = df.join(df, on=['hash'], how='left')  ; df1.show(3)

df1 = df.join(df, on=['hash'], how='right') ;df1.show(3)

df1 = df.join(df, on=['hash'], how='full') ; df1.show(3)

df1 = df.join(df, on=['hash'], how='left_anti') ; df1.show(3)

df1 = df.join(df, on=['hash'], how='right_outer') ;df1.show(3)


# ## 4. Hint for broadcast join
## On broadcast join the smaller dataframe gets transmitted to all nodes , 
## which will reduce the shuffle while join.

df1 = df.join(broadcast(df), on=['hash'], how='inner')

df1 = df.join(df.hint("broadcast"), on=['hash'], how='inner')


### 5. Repartition and Coalesce
## Shuffle tranfer data between nodes
## Repartition - to reduce or increase number of partition which supposed to be equal, hence cause shuffle
## Coleasce - only to reduce the number of paaartition which will be not be equal size, no data shuffle between nodes

spark.conf.set("spark.sql.files.maxPartitionBytes", 1000000)
spark.conf.get("spark.sql.files.maxPartitionBytes")

df = spark.read.json(path)

df.rdd.getNumPartitions()


df = df.repartition(100)
df.rdd.glom().collect()
df.rdd.getNumPartitions()




df = spark.read.json(path)
df = df.coalesce(10)
df.rdd.glom().collect()
df.rdd.getNumPartitions()


### repartition by column
df = spark.read.json(path)
df = df.repartition(col("dt"))
df.rdd.getNumPartitions()


######################################################################################
# ## 6. Union and union all
df = spark.read.json(path)
df1 = df.union(df)
df1.count()
df1.distinct().count()



######################################################################################
###### 7. Create view on dataframe
# Temporary View
# Spark session scoped. A local table is not accessible from other clusters 
# (or if using databricks notebook not in other notebooks as well) and is not registered in the metastore.
df.createOrReplaceTempView("data")



# Global Temporary View
# Spark application scoped, global temporary views are tied to a system preserved temporary database global_temp. 
## This view can be shared across different spark sessions (or if using databricks notebooks, then shared across notebooks).
df.createOrReplaceGlobalTempView("my_global_view")
spark.sql("select * from global_temp.my_global_view").show(3)
spark.read.table("global_temp.my_global_view").show(3)



# Global Permanent View
# Persist a dataframe as permanent view. The view definition is recorded in the underlying metastore. 
##You can only create permanent view on global managed table or global unmanaged table. 
## Not allowed to create a permanent view on top of any temporary views or dataframe.
### Note: Permanent views are only available in SQL API — not available in dataframe API 

# ## 8. Partition by and bucketing 
# Paritioning by used when you want to store data as raw files partitioning on some column.
# Whereas bucketing is used when you store data as table, which will create internally metadata while saving data.

df.repartition(2).write.partitionBy('dt').parquet("partition")
df.write.bucketBy(1, 'dt').mode("overwrite").saveAsTable('buckets', format='parquet')




#######################################################################################
##### 9. Create StructType and StructFields 
# Struct type allows you to create desired data type or structure the data type of your columns
# Struct fields are like column in your datastructure

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]


schema = StructType([     StructField("firstname",StringType(),True),     StructField("middlename",StringType(),True),     StructField("lastname",StringType(),True),     StructField("id", StringType(), True),     StructField("gender", StringType(), True),     StructField("salary", IntegerType(), True)   ])
df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)


##################################################################
################## Nested Schema
structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]

structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

df2 = spark.createDataFrame(data=structureData,schema=structureSchema)

df2.printSchema()
df2.show(truncate=False)




##################################################################
# ## 10. fillna() & fill() – Replace NULL Values
filePath= root + "fill_na_example.csv"
df = spark.read.options(header='true', inferSchema='true').csv(filePath)

df.printSchema()
df.show(truncate=False)


df.fillna(value=0).show()
df.fillna(value=0,subset=["population"]).show()
df.na.fill(value=0).show()
df.na.fill(value=0,subset=["population"]).show()


df.fillna(value="").show()
df.na.fill(value="").show()

df.fillna("unknown",["city"])     .fillna("",["type"]).show()


df.fillna({"city": "unknown", "type": ""})     .show()

df.na.fill("unknown",["city"])     .na.fill("",["type"]).show()

df.na.fill({"city": "unknown", "type": ""})     .show()



##################################################################
# ## 11. Spark aggregrate functions
simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]


df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)

print("approx_count_distinct: " +       str(df.select(approx_count_distinct("salary")).collect()[0][0]))

print("avg: " + str(df.select(avg("salary")).collect()[0][0]))


df.select(collect_list("salary")).show(truncate=False)


df2 = df.select(countDistinct("department", "salary"))
df2.show(truncate=False)

df.select(first("salary")).show(truncate=False)

df.select(kurtosis("salary")).show(truncate=False)

df.select(max("salary")).show(truncate=False)

df.select(min("salary")).show(truncate=False)

df.select(mean("salary")).show(truncate=False)
df.select(variance("salary"),var_samp("salary"),var_pop("salary"))   .show(truncate=False)



##########################################################
# ## 12. Drop records containing null

filePath="fill_na_example.csv"
df = spark.read.options(header='true', inferSchema='true')           .csv(filePath)

df.printSchema()
df.show(truncate=False)


df.na.drop().show(truncate=False)

df.na.drop(how="any").show(truncate=False)

df.na.drop(subset=["population","type"])    .show(truncate=False)

df.dropna().show(truncate=False)




#########################################################
############# ## 13. Explode nested array

x = [
  ("James",[["Java","Scala","C++"],["Spark","Java"]]),
  ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
  ("Robert",[["CSharp","VB"],["Spark","Python"]])
]

df = spark.createDataFrame(data= x, schema = ['name','subjects'])
df.printSchema()
df.show(truncate=False)

df.select(df.name,explode(df.subjects)).show(truncate=False)

df.select(df.name,flatten(df.subjects)).show(truncate=False)


df = spark.createDataFrame(x, schema = [ 'name', 'subject'])
df.printSchema()
df.show()

####
df.select( df.name, explode(df.subject)).show()

df.select( df.name, flatten(df.subject)).show()




###############################################################
# ## 14. Broadcast RDD
states = {"NY":"New York", "CA":"California", "FL":"Florida"}
broadcastStates = spark.sparkContext.broadcast(states)

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]

rdd = spark.sparkContext.parallelize(data)

def state_convert(code):
    return broadcastStates.value[code]

result = rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).collect()

print(result)


########################################################
# ## 15. Window function to apply dense rank  ##########
simpleData = (("James", "Sales", 3000),     ("Michael", "Sales", 4600),      ("Robert", "Sales", 4100),       ("Maria", "Finance", 3000),      ("James", "Sales", 3000),        ("Scott", "Finance", 3300),      ("Jen", "Finance", 3900),        ("Jeff", "Marketing", 3000),     ("Kumar", "Marketing", 2000),    ("Saif", "Sales", 4100)   ) 
columns= ["employee_name", "department", "salary"]

df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)


windowSpec  = Window.partitionBy("department").orderBy("salary")


from pyspark.sql.functions import dense_rank
df.withColumn("dense_rank",dense_rank().over(windowSpec))     .show()






