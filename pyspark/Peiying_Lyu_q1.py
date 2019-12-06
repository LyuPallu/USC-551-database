import re
from pyspark import SparkContext
from collections import defaultdict
import sys
import os
from operator import add

sc = SparkContext()

input_ = sys.argv[1]
output_ = sys.argv[2]
f = open(output_,'w')

lines = sc.textFile(input_)

counts = lines.map(lambda x:x.split(',')).map(lambda x: (x[0],x[1], int(x[2]))).filter(lambda x: x[1].lower().startswith("bud"))\
            .map(lambda k:(k[0],k[2]))\
            .groupByKey().map(lambda x:(x[0],list(x[1])))\
            .filter(lambda x: max(x[1]) <= 5).map(lambda x: (x[0],len(x[1]))) 
                
output = counts.collect()


for v in output:

    f.writelines('%s \t %s' % (v[0], v[1]))
    f.writelines("\n")

    # print( '%s, %s' % (v[0], v[1]))
f.close()
sc.stop()
