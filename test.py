

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

spark.

spark.



##########################################################################
#### pyspark.sql.functions module    #####################################
from pyspark.sql.functions import  ( add_months, current_date, current_timestamp, date_add, date_format,
date_sub, date_trunc, datediff, dayofmonth, dayofweek, dayofyear, from_unixtime, from_utc_timestamp, hour,
minute, month, months_between, next_day, quarter, second,
to_timestamp, to_utc_timestamp, unix_timestamp, weekofyear, year,
trunc,   # Returns date truncated to the unit specified by the format.
window,   ###   pyspark.sql.types.TimestampType.   w = df.groupBy(window("date", "5 seconds")).agg(sum("val").alias("sum"))
last_day,   #  Returns the last day of the month which the given date belongs to.
)





#####  Numerics Operations #########################################
from pyspark.sql.functions import  ( abs, acos, asin, atan, atan2, cos, cosh, exp, factorial, log, log10, log1p,
log2, pow, sin, sinh, sqrt, tan, tanh, round, floor, ceil, bround, expm1,cbrt,
rint,     # double value that is closest in value to the argument and is equal to a mathematical integer.
signum,   #  Computes the signum of the given value.
hypot,  # (col1, col2)   # sqrt(a^2 + b^2)
rand,     ### rand uniform
randn,   # Generates a column with independent and identically distributed (i.i.d.) samples from the standard normal distribution.
)


##### string  #########################################################
from pyspark.sql.functions import  ( instr, lower, lpad, ltrim, regexp_extract, regexp_replace, rpad, rtrim,
split, substring, substring_index, trim, upper,
levenshtein,  # ()  # Levenstsin sds   df0.select(levenshtein('l', 'r').alias('d')).collect()
locate,       # Find position of elt in string
translate,    # reaplce
initcap,      # to cap   Translate the first letter of each word to upper ca
length,  #Computes the character length of string data
repeat,  #  creates an array containing a column repeated count times.  df.select(repeat(df.s, 3).alias('s')).collect()
reverse
)


#### Aggregation  #####################################################
from pyspark.sql.functions import  (
approxCountDistinct, approx_count_distinct, avg, corr, count, countDistinct, kurtosis, max, mean, min, skewness,
stddev, stddev, stddev_pop, stddev_pop, stddev_samp, stddev_samp, sum, sumDistinct, variance,
covar_pop,   ## covaraince
covar_samp,  ### coariance sample
var_pop, var_samp
)




##### Operations   ###################################################################
from pyspark.sql.functions import  (
isnan
isnull
expr   ### execute dynamically  df.select(expr("length(name)")).collect()
when    # df.select(when(df['age'] == 2, 3).otherwise(4).alias("age")).collect()
)


#### Array   #########################################################################
from pyspark.sql.functions import  ( array, array_contains, array_distinct, array_except, array_intersect, array_join, array_max, array_min,
array_position, array_remove, array_repeat, array_sort, array_union, arrays_overlap, arrays_zip, sort_array,
element_at,  ### arrdf.select(element_at(df.data, 1)).collect()ay
sequence,    ### Generate a sequence of integers from start to stop, incrementing by step. If step is not set, incrementing by 1 if start is less than or equal to stop, otherwise -1.
shuffle,     ### Shuffle of the array
size,        ### len of array , df.select(size(df.data)).collect()
slice ,      ###v Collection function: returns an array containing all the elements in x from index start (or starting from the end if start is negative) with the specified length.
)



##### json   ###################################################
from pyspark.sql.functions import  (
get_json_object, from_json, schema_of_json, json_tuple, to_json
)



##### map, dictionnary   ####################################################################
from pyspark.sql.functions import  (
create_map  ## dictionnary key,value df.select(create_map('name', 'age').alias("map")).collect()
map_concat
map_from_arrays
map_from_entries
map_keys
map_values
)

##### Ordering functions  #######################################################
from pyspark.sql.functions import  (
asc
asc_nulls_first
asc_nulls_last
dense_rank
desc
desc_nulls_first
desc_nulls_last
)


#### Window   ######################################################################
from pyspark.sql.functions import  ( lag, lead, percent_rank, cume_dist, ntil, rank
)


lag(col, count=1, default=None)  # Window function: returns the value that is offset rows before the current row
lead(col, count=1, default=None)   #   Window function: returns the value that is offset rows after the current row,
percent_rank
cume_dist   #  Window function: returns the cumulative distribution of values within a window partition
ntile(n=4)  #Window function: returns the ntile group id (from 1 to n inclusive) in an ordered window partition.
rank    # Window function: returns the rank of rows within a window partition.



#### Column   ##################################################################
from pyspark.sql.functions import  (
concat, concat_ws, collect_list, collect_set, explode, 
explode_outer, flatten, greatest, least, posexplode, posexplode_outer, struct
)


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
from pyspark.sql.functions import  (
grouping, grouping_id, first, last  )


grouping      #  df.cube("name").agg(grouping("name"), sum("age")).orderBy("name").show()
grouping_id   # df.cube("name").agg(grouping_id(), sum("age")).orderBy("name").show()  returns the level of grouping,
first   ###  1st row
last    ###  last row



#### Various
from pyspark.sql.functions import  (
  format_number, format_string, PandasUDFType, pandas_udfE, udf, broadcast, coalesce, col,  
  column, input_file_name, lit, row_number, 
  monotonically_increasing_id, spark_partition_id 
)



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





from pyspark.sql.types import ( DataType, NullType, StringType, BinaryType, BooleanType, DateType,
TimestampType, DecimalType, DoubleType, FloatType, ByteType, IntegerType, LongType, ShortType,
ArrayType, MapType, StructField, StructType )





from pyspark.sql.types import StringType



from pyspark.sql.functions import *


from pyspark.sql import functions as f, types as t


import findspark


findspark.init() 


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct,collect_list
from pyspark.sql.functions import collect_set,sum,avg,max,countDistinct,count
from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness 
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct
from pyspark.sql.functions import variance,var_samp,  var_pop


from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType,StructField, StringType, IntegerType,
                               ArrayType, DoubleType, BooleanType)




spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

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


df.printSchema()





from pyspark.sql.types import StringType, StructType, IntegerType, FloatType

from pyspark.sql import functions as f


f.last()

# Convenience function for turning JSON strings into DataFrames.
def jsonToDataFrame(json, schema=None):
  # SparkSessions are available with Spark 2.0+
  reader = spark.read
  if schema:
    reader.schema(schema)
  return reader.json(sc.parallelize([json]))



# Using a struct
schema = StructType().add("a", StructType().add("b", IntegerType()))       
events = jsonToDataFrame("""
{
  "a": {
     "b": 1
  }
}
""", schema)

display(events.select("a.b"))





