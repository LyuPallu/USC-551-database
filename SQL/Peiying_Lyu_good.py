import mysql.connector
import sys


output_file = sys.argv[1]
f = open(output_file,'w')

cnx = mysql.connector.connect(user='inf551',password='inf551', database='inf551')
cursor = cnx.cursor()

input_file = "Peiying_Lyu_d.sql"
fd = open(input_file,'r')
query = fd.read()
fd.close()

cursor.execute(query)
results = cursor.fetchall()

for i in results:
    # i = i +'\n'
    f.writelines(i)
    f.writelines('\n')

f.close()
