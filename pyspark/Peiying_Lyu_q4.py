import re
import sys
import os
from pyspark import SparkContext
from collections import defaultdict
from operator import add
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import mean
from pyspark.sql.types import *
# sc.stop()

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

input_1 = sys.argv[1]
input_2 = sys.argv[2]
input_3 = sys.argv[3]

output_ = sys.argv[4]
f = open(output_,'w')

line1 = sc.textFile(input_1)
line2 = sc.textFile(input_2)
line3 = sc.textFile(input_3)

#-------------------------------------------------------------
ds1 = line1.map(lambda x: x.split(",")).collect()
ds1 = sc.parallelize(ds1)
sch1 = StructType([StructField("Drinker", StringType(), True),
                                    StructField("Beer", StringType(), True)])
df1 = spark.createDataFrame(ds1, sch1)


ds2 = line2.map(lambda x: x.split(",")).collect()
ds2 = sc.parallelize(ds2)
sch2 = StructType([StructField("Drinker", StringType(), True),
                                    StructField("Bar", StringType(), True)])
df2 = spark.createDataFrame(ds2, sch2)


ds3 = line3.map(lambda x: x.split(",")).map(lambda x: (x[0],x[1])).collect()
ds3 = sc.parallelize(ds3)
sch3 = StructType([StructField("Bar", StringType(), True),
                                    StructField("Beer", StringType(), True)])
df3 = spark.createDataFrame(ds3, sch3)

#------------------------------------
half = df1.join(df2, [df1.Drinker == df2.Drinker] ).join(df3,[df2.Bar == df3.Bar, df1.Beer == df3.Beer])

rdd = half.rdd
rdd = rdd.map(lambda row: [str(c) for c in row])\
                    .map(lambda x: ((x[0],x[1],x[3]),1)).reduceByKey(add)\
                    .map(lambda x: (x[0][0],x[0][1]))
rdd = rdd.map(lambda x: ((x[0],x[1]),1)).reduceByKey(add)
rdd = rdd.map(lambda x: (x[0][0], x[0][1]))
output = rdd.collect()

#output
f.writelines("Drinker\tBeer\n")
for v in output:
    f.writelines('%s \t %s \n' % (v[0], v[1]))

f.close()
sc.stop()