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
counts = lines.map(lambda x: x.split(","))\
                .map(lambda x: (x[0], float(x[2])))
cnt = counts.aggregateByKey((0,0), lambda U,v : (U[0]+v, U[1]+1), \
                            lambda U1,U2 : (U1[0]+U2[0], U1[1]+U2[1]))\
            .map(lambda a: (a[0], float(a[1][0])/float(a[1][1])))

output = cnt.collect()

f.writelines("Bar\tAverage_price\n")
for v in output:
    # f.writelines('')
    f.writelines( '%s \t %s \n' % (v[0], v[1]))
    

    # print( '%s, %s' % (v[0], v[1]))
f.close()
sc.stop()