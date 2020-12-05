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


#### pyspark.sql.functions module
PandasUDFType
abs
acos
add_months
approxCountDistinctD
approx_count_distinct
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
asc
asc_nulls_first
asc_nulls_last
ascii
asin
atan
atan2
avg
base64
bin
bitwiseNOT
broadcast
bround
cbrt
ceil
coalesce
col
collect_list
collect_set
column
concat
concat_ws
conv
corr
cos
cosh
count
countDistinct
covar_pop
covar_samp
crc32
create_map
cume_dist
current_date
current_timestamp
date_add
date_format
date_sub
date_trunc
datediff
dayofmonth
dayofweek
dayofyear
decode
degrees
dense_rank
desc
desc_nulls_first
desc_nulls_last
element_at
encode
exp
explode
explode_outer
expm1
expr
factorial
first
flatten
floor
format_number
format_string
from_json
from_unixtime
from_utc_timestamp
get_json_object
greatest
grouping
grouping_id
hash
hex
hour
hypot
initcap
input_file_name
instr
isnan
isnull
json_tuple
kurtosis
lag
last
last_day
lead
least
length
levenshtein
lit
locate
log
log10
log1p
log2
lower
lpad
ltrim
map_concat
map_from_arrays
map_from_entries
map_keys
map_values
max
md5
mean

min
minute
monotonically_increasing_id
month
months_between
nanvl
next_day
ntile
pandas_udfE
percent_rank
posexplode
posexplode_outer

pow
quarter
radians
rand
randn
rank
regexp_extract
regexp_replace
repeat
reverse
rint

round
row_number
rpad
rtrim
schema_of_json
second
sequence
sha1
sha2
shiftLeft
shiftRight
shiftRightUnsigned
shuffle
signum
sin
sinh
size
skewness


slice
sort_array
soundex
spark_partition_id
split
sqrt
stddev
stddev_pop
stddev_samp
struct
substring
substring_index


sum
sumDistinct
tan
tanh
toDegreesD
toRadiansD
to_date
to_json
to_timestamp
to_utc_timestamp
translate
trim
trunc
udf
unbase64
unhex
unix_timestamp
upper
var_pop
var_samp
variance
weekofyear
when
window
year


##### pyspark.sql.streaming module
StreamingQuery
StreamingQueryManager
DataStreamReader
DataStreamWriter






