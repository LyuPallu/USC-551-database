############################################
# Date:      Oct. 16 th 2019
# Name:      Peiying Lyu 
# ID:        8109407016
# Course:    INF 551, HW2
############################################
import xml.dom.minidom
from xml.dom.minidom import parse
import xml.etree.ElementTree as ET
import sys
import re
import string
from collections import Counter, defaultdict

input_name = sys.argv[1]
output_name = sys.argv[2]
key_word = sys.argv[3]
result_name = sys.argv[4]

def handling_string(str_):
    str_list = []
    no_invalid_chr = str.maketrans("", "", string.punctuation.replace("'",""))
    tmp_text = str_.lower().translate(no_invalid_chr)
    for item in tmp_text.split():
        str_list.append(item)
    # print(str_list)
    return str_list


key_words = handling_string(key_word)

data_root = ET.parse(input_name).getroot()
index_root = ET.parse(output_name)
index = index_root.getroot() #cataglog

#results.xml
result_root = ET.Element('results')

dis = defaultdict(list)

for index_child in index.findall('keyword'):
    tmp_test = index_child.get('value')

    for w in key_words: 
        all_found = True
        # if tmp_test == w:
        if not re.search(r"\b"+w+r"\b", tmp_test): #！
            all_found = False
            

    if all_found == True:
        for tmp_keyword in index_child.findall('book'):
            tmp_id = tmp_keyword.get('id')
            tmp_tag = tmp_keyword.get('attr')

            dis[tmp_id].append(tmp_tag)
                    
# print(dis)
#-------------------------------------------------------------
for j in dis:
    dis[j] = list(set(dis[j]))  #！


for d in dis:   #id
    node_result = ET.SubElement(result_root, 'book')
    node_result.attrib = {'id': d}
    # print(d)    #
    for i in dis[d]:  #<element>
        result_element = ET.SubElement(node_result,i)
        
        data = data_root.find(f'.//book[@id="{d}"]/{i}') #<element>
        result_element.text = data.text
        # print(result_element.text)


dom = xml.dom.minidom.parseString(ET.tostring(result_root))
pretty_xml_as_string = dom.toprettyxml()
# print(pretty_xml_as_string)

fp = open(result_name,'w')
fp.write(pretty_xml_as_string)
fp.close()
