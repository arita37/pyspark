# -*- coding: utf-8 -*-
"""
https://stackoverflow.com/questions/53217767/py4j-protocol-py4jerror-org-apache-spark-api-python-pythonutils-getencryptionen
%SPARK_HOME%\python;%SPARK_HOME%\python\lib\py4j-<version>-src.zip:%PYTHONPATH%,

%SPARK_HOME%\python;%SPARK_HOME%\python\lib\py4j-<version>-src.zip:%PYTHONPATH%,
- just check what py4j version you have in your spark/python/lib folder) helped resolve this issue.




%SPARK_HOME%\python;%SPARK_HOME%\python\lib\py4j-0.10.7-src.zip



SPARK_HOME  =>  /opt/spark-3.0.0-bin-hadoop2.7
PYTHONPATH  =>  %SPARK_HOME%/python;%SPARK_HOME%/python/lib/py4j-0.10.9-src.zip;%PYTHONPATH%
PATH  => %SPARK_HOME%/bin;%SPARK_HOME%/python;%PATH%




import findspark
findspark.init()
# you can also pass spark home path to init() method like below
# findspark.init("/path/to/spark")


"""



import glob

flist = glob.glob( "ex2/*.py" )

for fname in flist :
    with open(fname, mode='r') as fi :
        ll = fi.readlines()

    with open('fall.py', mode='a') as f0 :
       f0.write("\n\n######################################################################################")
       f0.write("\n########" + fname + "##################################################\n")
       f0.writelines(ll)


####################################################################################################
####################################################################################################




coalesce(numPartitions)[source]
Returns a new DataFrame that has exactly numPartitions partitions.

Parameters:	numPartitions – int, to specify the target number of partitions
Similar to coalesce defined on an RDD, this operation results in a narrow dependency, e.g. if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of the 100 new partitions will claim 10 of the current partitions. If a larger number of partitions is requested, it will stay at the current number of partitions.

However, if you’re doing a drastic coalesce, e.g. to numPartitions = 1, this may result in your computation taking place on fewer nodes than you like (e.g. one node in the case of numPartitions = 1). To avoid this, you can call repartition(). This will add a shuffle step, but means the current upstream partitions will be executed in parallel (per whatever the current partitioning is).

>>> df.coalesce(1).rdd.getNumPartitions()
1


foreach(f)[source]
Applies the f function to all Row of this DataFrame.

This is a shorthand for df.rdd.foreach().

>>> def f(person):
...     print(person.name)
>>> df.foreach(f)



foreachPartition(f)[source]
Applies the f function to each partition of this DataFrame.

This a shorthand for df.rdd.foreachPartition().

>>> def f(people):
...     for person in people:
...         print(person.name)
>>> df.foreachPartition(f)




title : 5 tips for Cracking the DCSD, How-to Crack the DCSD,

why i gave...

I passed with Overall Score: 76% in second retake.
Passing score is 65%. If you fail first time, you will have one free retake.
Your score by percentage will be displayed immediately

Total 40 Questions with Multiple Formats answers like One of Many, Many of Many, Negated.
The Scala and Python exams are functionally identical

t1 focuse on spark arch & programing with DF ,

it covers 70% of exam syllabus
pie chart
t2 solve Spark code by hand!

Though exam pattern is objective exam focuse on coding details,
70% of the questions are based on codeing examples
Have some practice of writing code by hand to
use as many rough paper you can in exam,
I have done rough work for every single questions, that made me take 5 papers both sides!
Format of the exam. Select one item that is true or false.
Select multiple items that are true or false.
Given a code fragment, identify the result(s) it produces or identify errors it contains.
Given a desired goal, select the code fragment that produces those results or select the design or implementation that minimizes runtime issues or performance bottlenecks.

t3

t4 time management,

Time limit is 3 hours - take your time! Not realy...
time flies in
skip the long time consuming questions...
You can marke such questions for later reviw (option box)
t5 learn from multiple sources,

my git link,
other imp links...
Post:
This month pass recognized and tough DCSD.
my experience and five tips for exam seeker
Article
Git



#### API Part



#### pyspark.sql module
Module Context
SparkSession
Builder
SQLContext
HiveContextD
UDFRegistration
DataFrame
GroupedDataE
Column
Catalog
Row
DataFrameNaFunctions
DataFrameStatFunctions
WindowE
WindowSpecE
DataFrameReader
DataFrameWriter



##### pyspark.sql.types module
DataType
NullType
StringType
BinaryType
BooleanType
DateType
TimestampType
DecimalType
DoubleType
FloatType
ByteType
IntegerType
LongType
ShortType
ArrayType
MapType
StructField
StructType




##########################################################################
#### pyspark.sql.functions module    #####################################
date_add
date_format
date_sub
date_trunc
datediff
dayofmonth
dayofweek
dayofyear
add_months
hour
year
weekofyear
minute
month
months_between
next_day
from_unixtime
from_utc_timestamp
to_timestamp
to_utc_timestamp
current_date
current_timestamp
unix_timestamp
second
trunc   # Returns date truncated to the unit specified by the format.
quarter
window   ###   pyspark.sql.types.TimestampType.   w = df.groupBy(window("date", "5 seconds")).agg(sum("val").alias("sum"))
last_day   #  Returns the last day of the month which the given date belongs to.



#####  Numerics Operations #########################################
abs
acos
asin
atan
atan2
log
log10
log1p
log2
tan
tanh
sin
sinh
sqrt
cos
cosh
factorial
exp
pow
cbrt   # Computes the cube-root of the given value.
expm1
rint     # double value that is closest in value to the argument and is equal to a mathematical integer.
signum   #  Computes the signum of the given value.
round
floor
ceil
bround
hypot(col1, col2)   # sqrt(a^2 + b^2)
rand     ### rand uniform
randn   # Generates a column with independent and identically distributed (i.i.d.) samples from the standard normal distribution.



##### string  #######################################################
rpad
rtrim
split
trim
substring
substring_index
regexp_extract
regexp_replace
upper
lower
lpad
ltrim
instr
levenshtein()  # Levenstsin sds   df0.select(levenshtein('l', 'r').alias('d')).collect()
locate         # Find position of elt in string
translate   ### reaplce
initcap  # to cap   Translate the first letter of each word to upper ca
length  #Computes the character length of string data
repeat  #  creates an array containing a column repeated count times.  df.select(repeat(df.s, 3).alias('s')).collect()
reverse



#### Aggregation  #####################################################
avg
stddev
stddev_pop
stddev_samp
stddev
stddev_pop
stddev_samp
sum
sumDistinct
variance
kurtosis
count
countDistinct
skewness
max
mean
min
approxCountDistinctD
approx_count_distinct
corr
covar_pop   ## covaraince
covar_samp  ### coariance sample
var_pop
var_samp




##### Operations   ####################################################################
isnan
isnull
expr   ### execute dynamically  df.select(expr("length(name)")).collect()
when    # df.select(when(df['age'] == 2, 3).otherwise(4).alias("age")).collect()



#### Array   #########################################################################
array
array_contains
array_distinct
array_except
array_intersect
array_join
array_max
array_min
array_position
array_remove
array_repeat
array_sort
array_union
arrays_overlap
arrays_zip
sort_array
element_at  ### arrdf.select(element_at(df.data, 1)).collect()ay

sequence    # Generate a sequence of integers from start to stop, incrementing by step. If step is not set, incrementing by 1 if start is less than or equal to stop, otherwise -1.
shuffle   ### Shuffle of the array
size   ## len of array , df.select(size(df.data)).collect()
slice  ###v Collection function: returns an array containing all the elements in x from index start (or starting from the end if start is negative) with the specified length.


##### json   ###################################################
get_json_object
from_json
schema_of_json
json_tuple
to_json



##### map, dictionnary   ####################################################################
create_map  ## dictionnary key,value df.select(create_map('name', 'age').alias("map")).collect()
map_concat
map_from_arrays
map_from_entries
map_keys
map_values


##### Ordering functions  #######################################################
asc
asc_nulls_first
asc_nulls_last
dense_rank
desc
desc_nulls_first
desc_nulls_last



#### Window   ######################################################################
lag(col, count=1, default=None)  # Window function: returns the value that is offset rows before the current row
lead(col, count=1, default=None)   #   Window function: returns the value that is offset rows after the current row,
percent_rank
cume_dist   #  Window function: returns the cumulative distribution of values within a window partition
ntile(n=4)  #Window function: returns the ntile group id (from 1 to n inclusive) in an ordered window partition.
rank    # Window function: returns the rank of rows within a window partition.




#### Column   ##################################################################
concat(*col)      #  Concatenates multiple input columns together into a single column. The function works with strings, binary and
concat_ws(sep=";", *col)   #  speration Concatenates multiple input string columns together into a single string column, using the given separator.
collect_list   ##   df2.agg(collect_list('age')).collect()   Aggregate function: returns a list of objects with duplicates.
collect_set    ###  Aggregate function: returns a set of objects with duplicate elements eliminated.
explode   ## array --> column eDF.select(explode(eDF.intlist).alias("anInt")).collect()
explode_outer   ### array --> column  Unlike explode, if the array/map is null or empty then null
flatten   ## flatten array into flat  Collection function: creates a single array from an array of arrays
greatest   # Returns the greatest value of the list of column name df.select(greatest(df.a, df.b, df.c).alias("greatest")).collect()
least(col1, col2, col3)  # Returns the least value of the list of column names, skipping null values
posexplode(col )  # Returns a new row for each element with position in the given array or map.  eDF.select(posexplode(eDF.intlist)).collect()
posexplode_outer  ### explode array into new new row
struct  ## new struct columns,  df.select(struct('age', 'name').alias("struct")).collect()


#### Rows Agg operation  #######################################################
grouping      #  df.cube("name").agg(grouping("name"), sum("age")).orderBy("name").show()
grouping_id   # df.cube("name").agg(grouping_id(), sum("age")).orderBy("name").show()  returns the level of grouping,
first   ###  1st row
last    ###  last row





format_number
format_string
PandasUDFType
pandas_udfE   ## Vectorize UDF
udf
broadcast   #  Marks a DataFrame as small enough for use in broadcast joins.
coalesce    ###


col  ,  column #   a Column based on the given column name.
input_file_name()   #Creates a string column for the file name of the current Spark task.
lit
row_number
monotonically_increasing_id
spark_partition_id





##### Encoding  ###################################################
bin   ##  Returns the string representation of the binary value of the given column.
encode
decode
conv    # Convert a number in a string column from one base to another.  Convert a number in a string column from one base to another.
ascii
base64
hash
hex
sha1
sha2
md5
unbase64
unhex
crc32
soundex
degrees


#### bits operation  #############################################
shiftLeft
shiftRight
shiftRightUnsigned
bitwiseNOT


#### Angles   ####################################################
radians
toDegreesD
toRadiansD




###################################################################
##### pyspark.sql.streaming module  ################################
StreamingQuery
StreamingQueryManager
DataStreamReader
DataStreamWriter






