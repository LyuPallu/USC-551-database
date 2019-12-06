import re
from pyspark import SparkContext
from collections import defaultdict
import sys
import os
from operator import add

sc = SparkContext()

input_1 = sys.argv[1]
input_2 = sys.argv[2]

output_ = sys.argv[3]
f = open(output_,'w')

line1 = sc.textFile(input_1)
line2 = sc.textFile(input_2)

ds1 = line1.map(lambda x: x.split(",")).collect()
ds1 = sc.parallelize(ds1)
ds2 = line2.map(lambda x: x.split(",")).collect()
ds2 = sc.parallelize(ds2)

full = ds1.leftOuterJoin(ds2).filter(lambda x: x[1][1] == None)\
        .map(lambda x: x[0])

output = full.collect()
f.writelines("Drinker\n")
for v in output:
    f.writelines(v)

f.close()
sc.stop()