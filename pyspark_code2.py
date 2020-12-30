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
import boto3
import os 


# In[2]:


spark = SparkSession.builder.master("local").appName("PySpark").getOrCreate()


# In[3]:


path = "test-table/"


# In[55]:


df = spark.read.json(path)


# In[51]:


df.printSchema()


# In[5]:


df.createGlobalTempView("data")


# In[6]:


df.show(3)


# ## 1. Cache & Persist

## Here cache and persist both works same way , 
## but persist allows you to choose storage of your choice.

# In[7]:


df.cache()


# In[7]:


df.cache().explain(1000)


# In[8]:


df.persist(StorageLevel.MEMORY_ONLY)


# In[9]:


df.persist(StorageLevel.MEMORY_ONLY_SER)


# In[10]:


df.persist(StorageLevel.MEMORY_AND_DISK)


# In[11]:


df.persist(StorageLevel.DISK_ONLY)


# ## 2. Unpersist or remove all cached dataframes

# In[12]:


spark.catalog.clearCache()


# In[16]:


spark.catalog.dropGlobalTempView("data")


# In[14]:


df.unpersist()


# ## 3. Types of joins

# In[17]:


df1 = df.join(df, on=['hash'], how='left')


# In[19]:


df1.show(3)


# In[20]:


df1 = df.join(df, on=['hash'], how='right')


# In[21]:


df1.show(3)


# In[22]:


df1 = df.join(df, on=['hash'], how='full')


# In[23]:


df1.show(3)


# In[24]:


df1 = df.join(df, on=['hash'], how='left_anti')


# In[25]:


df1.show(3)


# In[27]:


df1 = df.join(df, on=['hash'], how='right_outer')


# In[29]:


df1.show(3)


# ## 4. Hint for broadcast join

## On broadcast join the smaller dataframe gets transmitted to all nodes , 
## which will reduce the shuffle while join.

# In[35]:


df1 = df.join(broadcast(df), on=['hash'], how='inner')


# In[36]:


df1 = df.join(df.hint("broadcast"), on=['hash'], how='inner')


# ## 5. Repartition and Coalesce

## Repartition - to reduce or increase number of partition which supposed to be equal, hence cause shuffle
## Coleasce - only to reduce the number of paaartition which will be not be equal size

# In[45]:


spark.conf.set("spark.sql.files.maxPartitionBytes", 1000000)
spark.conf.get("spark.sql.files.maxPartitionBytes")


# In[57]:


df = spark.read.json(path)


# In[58]:


df.rdd.getNumPartitions()


# In[59]:


df = df.repartition(100)


# In[60]:


df.rdd.glom().collect()


# In[61]:


df.rdd.getNumPartitions()


# In[62]:


df = spark.read.json(path)


# In[63]:


df = df.coalesce(10)


# In[64]:


df.rdd.glom().collect()


# In[65]:


df.rdd.getNumPartitions()


# In[66]:


df = spark.read.json(path)


# In[67]:


df = df.repartition(col("dt"))


# In[68]:


df.rdd.getNumPartitions()


### 6. Union

## Union will eliminate duplicate records

# In[81]:


df = spark.read.json(path)


# In[69]:


df1 = df.union(df)


# In[71]:


df1.count()


# In[77]:


df1.distinct().count()


# ## 7. Create view on dataframe

# Temporary View
# 
# Spark session scoped. 
# A local table is not accessible from other clusters (or if using databricks notebook not in other notebooks as well) and is not registered in the metastore.
# 

# In[84]:


df.createOrReplaceTempView("data")


# Global Temporary View
# 
# Spark application scoped, global temporary views are tied to a system preserved temporary database global_temp. 
# This view can be shared across different spark sessions (or if using databricks notebooks, then shared across notebooks).
# 

# In[88]:


df.createOrReplaceGlobalTempView("my_global_view")
spark.sql("select * from global_temp.my_global_view").show(3)
spark.read.table("global_temp.my_global_view").show(3)


# Global Permanent View
# 
# Persist a dataframe as permanent view. The view definition is recorded in the underlying metastore. 
# You can only create permanent view on global managed table or global unmanaged table. 
# Not allowed to create a permanent view on top of any temporary views or dataframe. 
# Note: Permanent views are only available in SQL API — not available in dataframe API
# 

### 8. Partition by and bucketing 
# 
# Paritioning by used when you want to store data as raw files partitioning on some column.
# Whereas bucketing is used when you store data as table, which will create internally metadata while saving data.
# 


df.repartition(2).write.partitionBy('dt').parquet("partition")


# In[107]:


df.write.bucketBy(1, 'dt').mode("overwrite").saveAsTable('buckets', format='parquet')


# ## 9. Create StructType and StructFields
# 
# Struct type allows you to create desired data type or structure the data type of your columns
# Struct fields are like column in your datastructure
#

# In[108]:


from pyspark.sql.types import StructType,StructField, StringType, IntegerType


# In[109]:


data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]


# In[110]:


schema = StructType([StructField("firstname",StringType(),True),
        StructField("middlename",StringType(),True),
        StructField("lastname",StringType(),True),
        StructField("id", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("salary", IntegerType(), True)])


# In[111]:


df = spark.createDataFrame(data=data,schema=schema)


# In[112]:


df.printSchema()
df.show(truncate=False)


# Nested Schema

# In[113]:


structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]


# In[114]:


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


# In[115]:


df2 = spark.createDataFrame(data=structureData,schema=structureSchema)


# In[116]:


df2.printSchema()
df2.show(truncate=False)


# ## 10. fillna() & fill() – Replace NULL Values

# Fill na or na.fill works to fill null 
# values of given column in your df.
#

# In[118]:


filePath="fill_na_example.csv"
df = spark.read.options(header='true', inferSchema='true')           .csv(filePath)

df.printSchema()
df.show(truncate=False)


# In[119]:


df.fillna(value=0).show()
df.fillna(value=0,subset=["population"]).show()
df.na.fill(value=0).show()
df.na.fill(value=0,subset=["population"]).show()


# In[120]:


df.fillna(value="").show()
df.na.fill(value="").show()


# In[121]:


df.fillna("unknown",["city"])     .fillna("",["type"]).show()


# In[122]:


df.fillna({"city": "unknown", "type": ""})     .show()

df.na.fill("unknown",["city"])     .na.fill("",["type"]).show()

df.na.fill({"city": "unknown", "type": ""})     .show()


# ## 11. Spark aggregrate functions

# In[124]:


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


# In[125]:


df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)

print("approx_count_distinct: " +       str(df.select(approx_count_distinct("salary")).collect()[0][0]))

print("avg: " + str(df.select(avg("salary")).collect()[0][0]))


# In[126]:


df.select(collect_list("salary")).show(truncate=False)


# In[127]:


df.select(collect_set("salary")).show(truncate=False)


# In[130]:


df2 = df.select(countDistinct("department", "salary"))
df2.show(truncate=False)


# In[131]:


df.select(first("salary")).show(truncate=False)


# In[132]:


df.select(last("salary")).show(truncate=False)


# In[133]:


df.select(kurtosis("salary")).show(truncate=False)


# In[134]:


df.select(max("salary")).show(truncate=False)


# In[135]:


df.select(min("salary")).show(truncate=False)


# In[136]:


df.select(mean("salary")).show(truncate=False)
df.select(variance("salary"),var_samp("salary"),var_pop("salary"))   .show(truncate=False)


# ## 12. Drop records containing null

# na.drop or drop na remove records having null but 
# in addition to that na.drop allows you to apply based on
# specific column groups.

# In[137]:


filePath="fill_na_example.csv"
df = spark.read.options(header='true', inferSchema='true')           .csv(filePath)

df.printSchema()
df.show(truncate=False)


# In[138]:


df.na.drop().show(truncate=False)


# In[139]:


df.na.drop(how="any").show(truncate=False)


# In[140]:


df.na.drop(subset=["population","type"])    .show(truncate=False)


# In[141]:


df.dropna().show(truncate=False)


# ## 13. Explode nested array

# Explode array allows you to convert array values in 
# column to records.
# This function is helpful when you have aggregated values in one column.

# In[143]:


arrayArrayData = [
  ("James",[["Java","Scala","C++"],["Spark","Java"]]),
  ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
  ("Robert",[["CSharp","VB"],["Spark","Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjects'])
df.printSchema()
df.show(truncate=False)


# In[146]:


df.select(df.name,explode(df.subjects)).show(truncate=False)


# In[150]:


df.select(df.name,flatten(df.subjects)).show(truncate=False)


# ## 14. Broadcast RDD

# Broadcast rdd is a spark optimization technique used
# to propogate small data frame or rdd to reach node and 
# do operations without any shuffle in data.

# In[151]:


states = {"NY":"New York", "CA":"California", "FL":"Florida"}
broadcastStates = spark.sparkContext.broadcast(states)

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]


# In[152]:


rdd = spark.sparkContext.parallelize(data)

def state_convert(code):
    return broadcastStates.value[code]

result = rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).collect()


# In[154]:


print(result)


# ## 15. Window function to apply dense rank

# Dense rank is helpful when you need to
# provide rank to even duplicate values and filter them out.

# In[156]:


simpleData = (("James", "Sales", 3000),     ("Michael", "Sales", 4600),      ("Robert", "Sales", 4100),       ("Maria", "Finance", 3000),      ("James", "Sales", 3000),        ("Scott", "Finance", 3300),      ("Jen", "Finance", 3900),        ("Jeff", "Marketing", 3000),     ("Kumar", "Marketing", 2000),    ("Saif", "Sales", 4100)   )
 
columns= ["employee_name", "department", "salary"]

df = spark.createDataFrame(data = simpleData, schema = columns)

df.printSchema()
df.show(truncate=False)


# In[158]:


windowSpec  = Window.partitionBy("department").orderBy("salary")


# In[159]:


from pyspark.sql.functions import dense_rank
df.withColumn("dense_rank",dense_rank().over(windowSpec))     .show()


# In[ ]:




