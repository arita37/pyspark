import findspark
findspark.init()


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales",   4600),
    ("Robert", "Sales",    4100),
    ("Maria", "Finance",   3000),
    ("James", "Sales",     3000),
    ("Scott", "Finance",   3300),
    ("Jen", "Finance",     3900),
    ("Jeff", "Marketing",  3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]


df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()



from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


#### Sliding window part
ws = Window.partitionBy('department').orderBy('salary')

df.withColumn('row_num', row_number().over(ws)  ).show(5)


from pyspark.sql.functions import rank, dense_rank

df.withColumn('rank1',  dense_rank().over(ws)  ).show()



ws_agg = Windows.partitionBy('department')



