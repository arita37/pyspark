apparach spark 2.4


Question 1
) Which of the following is an narrow transformation?
Coalesce
Group by
Map
Filter
collect


group by : not narrow  (ie shuffle  ---> right
collect :     not arrow    ( retrieve all results on the driver ???  → right
map  :       apply function to the RDD/dataframe  : narrow → rght
Filter :       fitler rows by criteria (yes)  : narrow → rght

reduceByKey,  : not narrow
Coalesce ?  :      not narrow (? shuffle involved ??) between workers shuffle takes place
                         So its also wide transformation

RDD and Dataframe : same ? (for transformation ?)
Dataframes when compiled they get converted to RDD only underlying so all transformations what we do on df execute on RDD : ok logic is on RDD.
https://sparkbyexamples.com/apache-spark-rdd/spark-rdd-transformations/
     --> data moved : wide trans   ,    data no moved : narrow (Narrow tfr)

Can you point doc here :  https://sparkbyexamples.com/   ? ok
I dont see dataframe functions examples in that website
https://sparkbyexamples.com/spark/spark-sql-map-functions/
https://sparkbyexamples.com/apache-spark-rdd/spark-rdd-transformations/
In exam some questions will have multiple answer options

Sort: wide transformation,  Implies crossing of partition ??/
        Yes sort means to move matching key data to other workers which is also called as shuffling

Narrow transformation : If one partition does NOT depends on other partition data(it might be present in same worker or other worker machine as well)
Wide tfr :  If one partition  depends on other partition data


TRANSFORMATION METHODS	METHOD USAGE AND DESCRIPTION
cache()	         Caches the RDD
filter()	            Returns a new RDD after applying filter function on source dataset.
flatMap(           Returns flattern map meaning if you have a dataset with array, it converts each elements in a array as a row. In other words it return 0 or more items in output for each element in dataset.
map()	             Applies transfo function on dataset and returns same number of elements in distributed dataset.
mapPartitions()	Similar to map, but executs transfor function on each partition, This gives better performance than map function
mapPartitionsWithIndex()  Similar to map Partitions, but also provides func with an integer value representing the index of the partition.
randomSplit()	Splits the RDD by the weights specified in the argument. For example rdd.randomSplit(0.7,0.3)
union()	             Comines elements from source dataset and the argument and returns combined dataset.
sample()	Returns the sample dataset.
intersection()	Returns the dataset which contains elements in both source dataset and an argument
distinct()	Returns the dataset by eliminating all duplicated elements.
repartition()	Return a dataset with number of partition specified in the argument. This operation reshuffles the RDD randomly, It could either return lesser or more partioned RDD based on the input supplied.
coalesce()	Similar to repartition by operates better when we want to the decrease the partitions. Betterment acheives by reshuffling the data from fewer nodes compared with all nodes by repartition.



https://data-flair.training/blogs/apache-spark-rdd-vs-dataframe-vs-dataset/


Question 2

We want to have 4 output files when we write the dataframe. Please fill below code part.
Please write the dataframe with 4 output files
        df  = spark.read.csv(“file_path”)


We i took exam we had code as well but current one has only mcs
But if you are familiar with code part you can save your time

Solution 1 :
      df.repartition(4)
                   .write.csv("path/myfile")

Solution 2 :
df.coalese(4).write.mode("SaveMode.Append").csv("path/mfile")   ?

Yes this looks good
by SQL using Hive may be possible... (?), but looks complicated.
Instead of repartition can i use any other option ?


whats difference ???
Coalesce and repartition both shuffles the data to increase or decrease the partition, but repartition is more costlier operation as it performs full shuffle.

Repartition : will shuffle data in each workers partition and then between workers as well (which is very costly task) to make correctly distributed.
Coalesce :   will shuffle between workers only
Hence using coalesce is recommended  if we want to choose output partition  files

Benefit of repartition ?? :  to avoid many small files getting created which will impact the performance(using partition while writing).  And some time if your data is not evenly distributed in works then manually we can repartition

By default spark will have 128mB size block of partitions.

Only 2 ways of shuffle :     repartition  and coalesce
All wide transformations :   (ok, they are implicit using shuffle, their goal is not the shuffle)
groupBy
sortBy
Aggregateby
Collect



Question 3)

Which of the following spark-submit mode option user should use if he want to run driver process on the client machine(from where he submits the job)
Client mode
Cluster mode
Num-executors
Driver-memory

https://docs.cloudera.com/runtime/7.2.1/running-spark-applications/topics/spark-submit-options.html
Spark jobs are ran using spark-submit tool in production cluster

The above mentioned options are all of spark-submit tools

deploy-mode
      Client mode : machine from which we submit our spark job. On that machine driver container will be running
      Cluster mode : all driver and workers will be running on other machines in the cluster

num-executors : option using to set no of executors for the application (== nb of nodes ??)
    Executors are called as process which runs the application transformations
    One node can have more then one executor

driver-memory : option used to overwrite the default setting of driver-memory

The answer is client mode
       Yes generally the question options are very much confusion in the exam

we use while spark-submit
# spark-submit
   commands.








1) basic command with default settings.


   --master local[*]


   --deploy-mode client


   --driver-memory 1G


   --executor-memory 1G


   --queue default


   --num-executors 2


$> spark-submit python_file.py


2) with custom settings.






 1)   we want to use resource manager as yarn & we want driver program to run on the same job submitted m/c with driver memory as 2G & executor memory as 2G


      & number of executor to be 5





   $> spark-submit --master yarn --deploy-mode client --driver-memory 1G --driver-memory 2G --executor-memory 2G python_file.py







 2)    we want to use resource manager as yarn & we want driver program to run on one of the yarn container with driver memory as 2G & executor memory as 2G


      & number of executor to be 5







   $> spark-submit --master yarn --deploy-mode cluster --driver-memory 1G --driver-memory 2G --executor-memory 2G python_file.py











   3) If you want to download some maven jars which are used by spark-application.


   $> spark-submit --packages group-id:artifact-id:version python_file.py







   4) If we want to provide an external jar.


   $> spark-submit --jars path_to_jar_file python_file.py







   5) If we want to supply config files.


   $> spark-submit --files confi_file_path python_file.py





Q4) which of the following are narrow transformation
Take
Collect
Count


Count : narrow (not cross shuffle)
Collect : not Narrow
take :   Narrow  (not cross shuffle)

for row in rows.take(rows.count()): print(row[1])
   First Name
   DAVID

Take : will take sample n records ,  Yes great take is narrow just










Question 1 :
1) Array explode and select first item from another array
sample-input:
title,authors
The Definitive Guide,[Bill Chambers,Matei Zaharia]
Lightning-Fast Big Data Analysis,[Holden Karau,Andy Konwinski,Patrick Wendell,Matei Zaharia]
Spark in Action, [Jean-Georges Perrin]


output
The Definitive Guide,Bill Chambers
Lightning-Fast Big Data Analysis,Holden Karau
Spark in Action,Jean-Georges Perrin

Can you try this one ?

 df  = spark.read.csv(“file_path”)
from pyspark.sql.functions import explode
df.withColumn("authors", explode(df.authors)))

// till this it looks good. TO select first element from array just you need to use index on column like below
df1 = df.withColumn("authors", explode(df.authors))).select(“title”,”authors[0]”)
df1.show()

https://sparkbyexamples.com/pyspark/pyspark-explode-nested-array-into-rows/
THis is the answer








Question
   Remove duplicates and sort in dec order with nulls first

sample input

id,name,age
1,jack,32
2,mike,54
null,mark,23
4,jordhan,44
4,jordhan,null
null,john,22

Output
id,name,age
null,john,22
null,mark,23
1,jack,32
2,mike,54
4,jordhan,44
4,jordhan,null


Answer:
df2 = df.distinct()
df2 = df2.sort( df2.sort.asc() ) // this will not sort by nulls first.
df2.show()
OK ?????
https://sparkbyexamples.com/pyspark/pyspark-orderby-and-sort-explained/


To have null columns sort first we should use its functions.
Df2 = df2.orderBy(df.id.asc_nulls_first())
df2.show()

asc_nulls_last() // to have nulls are last
No you can try sql as well it will not work with nulls
### Using Spark SQL, OK ?
df.createOrReplaceTempView("EMP")
ss = " select distinct id, name, age from EMP ORDER BY id asc "
spark.sql(ss)

Any queries ?

Think SQL works.... (anyway, it

Where to find API docs ???  asc_nulls_first()
https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
In functions package these functions are there
Question
Split a dataframe into 2 dataframes(80% and 20%) (order does not matter) and calculate summary on 1st dataframe


Answer
df_split = df.randomSplit(Array(0.8, 0.2))
df2 = df_split(0)
df3 = df_split(1)

df2.describe() // df2.summary()

Yes its almost correct
OK ?
calculate stats on 1st column????????????
  PLEASE MAKE A CORRECT QUESTION OK ?
I need correct question, OK ?
Yes,



Yes its looks good yaki

Usually in spark we have only one method to split the dataframe and in exam they dont mention it

OK, now I see... thats not good logic.
 (you can split dataframe by following sequential id order. )
Suppose you have streaming data, into datraframe, you split sequentially.

Lets do next question.
ok




Question:
 read the json file by providing custom schema while reading the files


sample_json = {
    "name": "jack",
    "age": 54,
    "qualification": "MBA"
}

form pyspark.sql.types import StructType, StructField,StringType, IntegerType

jsonschema = StructType().add("name", StringType, True).add('age', IntegerType, True).add('qualification', StringType, True)

df_schema = spark.read.schema.(jsonschema).json("myjsonpath")
df,printSchema()
df.describer()


https://sparkbyexamples.com/pyspark/pyspark-structtype-and-structfield/

OK ?
Yes its correct

Can we use this one ?
Yes we can use below as well but its complex way
You need to store column names and types in schema.json file
No really.
In exam they ask only ways using spark functions we can implement

Do we have thise kind of question ??? (generic way to load json ??)

Below example is more towards scala native function way
OK, got it.

https://sparkbyexamples.com/spark/spark-read-json-with-schema/
url = ClassLoader.getSystemResource("schema.json")
schemaSource = Source.fromFile(url.getFile).getLines.mkString
schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]

df2 = spark.read.schema(schemaFromJson )
       .json("src/main/resources/zipcodes.json")
df2.printSchema()
df2.show(false)

ddlSchemaStr = "`fullName` STRUCT<`first`: STRING, `last`: STRING,
`middle`: STRING>,`age` INT,`gender` STRING"
ddlSchema = StructType.fromDDL(ddlSchemaStr)
 ddlSchema.printTreeString()



Question
 read multiline json file.

sample json data.
{
    "name": "jack","age": 54,"qualification": "MBA"
},
{
    "name": "mark","age": 34,"qualification": "BA"
}


df = spark.read.option("multiline", 'true').json("myjson.json")
df.printSchema()
df.describe()

OK ?
Yes its correct



Question
On Spark dataframe add new Column and rename existing column,
You can hardcode 54 record value in age column

input columns:
name
Jack

output
Full Name, age
Jack,54

They dont ask this kind of question ??? (too simple ) ??? they ask 30 % simple, 40% intermidiate and 30% hard q’s

@udf(returnType=IntegerType())
def myval():
   return 54
# sqlContext.udf.register('myval_udf', myval )

df = df.withColumnRenamed('name', 'Full Name').withColum('age', myval )

import pyspark.sql.functions as f
# define the function
def myval():
   return 54

# make the udf version
myval_udf = f.udf(myval)
df = df.withColumnRenamed('name', 'Full Name').withColum('age', myval_udf )


from pyspark.sql.function import lit
df = df.withColumnRenamed('name', 'Full Name').withColum('age', lit(54) )

Hint : we need to use lit() functions to provide values
If you want to use myval function then you need to register it as udf

ok ? // whatever you tried to register udf is if you want to use udf in sql fashion

@udf(returnType=IntegerType())
... def add_one(x):
...    if x is not None:
...        return x + 1
You need to register in the example fashion

Any queries ?

what about lit(54) ???

Ok
Lit funcation is used if we want to hardcore any value for a column
df = df.withColumnRenamed('name', 'Full Name').withColum('age', lit(54) )

Full Name,age
Jack , 54

Any queries yaki ?


No, it's fine, ok
we can harcode default value. for new column...// yes




Question
    Split the full name column into first name and last name columns

input
fullname
Bill Chambers
Matei Zaharia

output
firstname,lastname
Bill, Chambers
Matei, Zaharia


####################
import pyspark.sql.functions as f

# define the function
def myval(x):
   try :
      return x.split(" ")[1]
   except :
       return ''

# make the udf version
myval_udf = f.udf(myval)


def myval2(x):
   try :
      return x.split(" ")[0]]
   except :
       return ''

# make the udf version
myval_udf2 = f.udf(myval2)


df = df..withColum('lastname', myval_udf('fullname')  )
df = df.withColum('firstname', myval_udf2('fullname')  )
df = df.select([ 'firstname', 'lastname' ])

ok ? yes this works but can you try some efficient way as well ?
Without udf we can even make use of builtin fuctions

split_col = pyspark.sql.functions.split(df['fullnamel'], ' ')
df = df.withColumn('firtname', split_col.getItem(0))
df = df.withColumn('firtname', split_col.getItem(1))
df = df.select([ 'firstname', 'lastname' ])

ok ? It looks good this time
But you have used scala way not pyspark way , that shd be fine
How is the pyspark equivalent ? 9correct one???
split_col = pyspark.sql.functions.split(df['fullnamel'], ' ')
split_col.withColumn(“firstname”, split_col.fullnamel[0]).withColumn(“lastname”, split_col.fullname[1])


Above is equivalent
fullname can be called as array directly ???
Yes when we use split() → it return array
Eg :
Bill, Chambers → [Bill, Chambers]

Yes correct
there is syntax error above... ????
now, ok ? fullname cannot be created without assignment....  fullname is assigned to split_col.....

Yes you are correct
ok, thank you





Get the default parallelism in spark cluster.

spark.default.parallelism  gives the value
    local mode : # cores on the machine
    mesos :  8
  others = max(2, nb of cores)

https://dzone.com/articles/apache-spark-performance-tuning-degree-of-parallel

OK ?  ??????? yes it looks good





















Hello,

lets start now, we have 1 hour late....

Hello yaki


11) create dataframe from a give list

Can you try this out ?



dataDF = [(('James','','Smith'),'1991-04-01','M',3000),
  (('Michael','Rose',''),'2000-05-19','M',4000),
  (('Robert','','Williams'),'1978-09-05','M',4000),
  (('Maria','Anne','Jones'),'1967-12-01','F',4000),
  (('Jen','Mary','Brown'),'1980-02-17','F',-1)
]

spark = SparkSession.builder.appName('myapp').getOrCreate()

df = spark.createDataframe(data = dataDF)


ok ?

Yes its correct


Can we to next question ?
_______________________________________________________________________________

a bit more difficult please
Sure

Encode a given column with UTF-16 encoder.
I will paste docs and inputs give me a minute please



OK, please try to prepare things in advance,
based on your personal Studies/notes you did when you passed exams.










UDF parts

def myfunction():



myfun_udf = udf( lambda x


Error Handking Null check inside the UDF

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, LongType
from pyspark.sql.function import col, udf


####
spark  = SparkSession.builder.appName("SparkBy").getOrCreate()
cols = ['col1', 'col2' ]

####  in Datafrrame usage
myfun_udf = udf( lambda x : myfun(x), StringType)
df.withColumn('mynewcol", myfun_df(col('Name'))).show(truncate=False)


#### SQL USAGE
spark.udf.register('my_fun_udf', myfun, StringType())
df.createOrReplaceTempView('tmpveiew)
spark.sql("select  id, myfun_udf(Name) " ).show()


null side effect :

This WHERE clause does not guarantee the strlen UDF to be invoked after filtering out nulls.
To perform proper null checking, we recommend that you do either of the following:
Make the UDF itself null-aware and do null checking inside the UDF itself
Use IF or CASE WHEN expressions to do the null check and invoke the UDF in a conditional branch


spark.udf.register("strlen_nullsafe", lambda s: len(s) if not s is None else -1, "int")

spark.sql("select s from test1 where s is not null and strlen_nullsafe(s) > 1") // ok
spark.sql("select s from test1 where if(s is not null, strlen(s), null) > 1")   // ok








#############################

1) To read 1TB of data which of the following configurations will process faster.

a) 4-Node Cluster (1TB ram), 2 executors of 16gb ram
b) 4-Node Cluster (1TB ram), 4 executors of 16gb ram
c) 4-Node Cluster (1TB ram), 6 executors of 16gb ram
d) 4-Node Cluster (1TB ram), 10 executors of 16gb ram


2) How would you get the number of partitions of a dataframe df.
a) df.rdd.getNumPartitions
b) df.getNumPartitions
c) df.showNumPartitions
d) None


3) Below which code snippet will have better Performance while filtering after Cacheing.
a)
    df.cache()
    df.filter("country = 'japan'").count()
b)
    df.cache()
    df.count()
    df.filter("country = 'japan'").count()
c) Cacheing doesnt improve the Performance.
d) None


4) How to get count of distinct records of a dataframe?
a) df.distinct()
b) df.distinct.count()
c) not supported
d) none of the above


5) df is a dataframe having 1000 of records. You need to look only 10 records. How would you get it done?
a) df.take()
b) df(10).show()
c) df.collect()
d) df.take(10)


6) While developing a code, it is needed to see the datatype of all columns of dataframe. How would you get his information?
a) df.dtypes
b) df.columns
c) df.show()
d) df.take()


7) Lets say you have a dataframe "df" with all columns as string datatype. It have few null values. It is needed to replace
all null values with "NA". What is the correct syntax to replace null values with "NA"?

a) UDF is required to achieve it.
b) df.fill("NA")
c) df.na.fill("NA")
d) df.withColumnRenamed(null,"NA")


8) There is table in hive named as "products". What is the correct syntax to load this table into spark dataframe using scala?

a) tbl = spark.load("products")
b) tbl = spark.table("products")
c) tbl = spark.sql("products")
d) tbl = spark.createDataFrame("products")


9) Which of the following 3 DataFrame operations are classified as a wide transformation (that is, they result in a shuffle)?
Choose 3 answers:
a) filter()
b) orderBy()
c) cache()
d) distinct()
e) repartition()
f) drop()


10) Which of the following 3 DataFrame operations are NOT classified as an action?
Choose 3 answers:
a) printSchema()
b) cache()
c) first()
d) show()
e) limit()
f) foreach()

























----------------------------------------------------------------------------------------------------------------------------------------------
Patch 1
solution

1) option d
reason : more the executors more parallel the data processed

2) option a
reason : we dont have direct method avaiable on dataframe to get the number of partitions hence we should convert to rdd and use the getNumPartitions method.

3) option b
reason : after running first action the data will be cached in the memory, there after Performance will be improved.

4) option b
df.distinct.count()

5) option d
df.take(10)

6) option a
df.dtypes

7) option c
df.na.fill("NA")

8) option c
tbl = spark.table("products")

9) option b,d,e

10) option a,b,e

























