############################################
# Date:      Oct. 16 th 2019
# Name:      Peiying Lyu 
# ID:        8109407016
# Course:    INF 551, HW2
############################################
import xml.dom.minidom
from xml.dom.minidom import parse
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
import sys
import string

def handling_string(str_):
    str_list = []
    char = '<>?,./:";{}|[]\~!@#$%^&*()_+`-='
    no_invalid_chr = str.maketrans("", "", char)
    tmp_text = str_.lower().translate(no_invalid_chr)
    for item in tmp_text.split():
        str_list.append(item)

    # str_list = str_.split(','  '-' or ' ')
    return str_list

input_name = sys.argv[1]
output_name = sys.argv[2]

# #books.xml
DOMTree = ET.parse(input_name)
catalog = DOMTree.getroot()

#index.xml
doc = xml.dom.minidom.Document()
root = ET.Element('index')
#-------------------------------------------
dis = defaultdict(set) 

books = catalog.findall('book')
for book in books:   #root <element>

    tmp_id = book.get('id')            #each id
    for child in book: #<element>
        if child.tag == 'price' or child.tag == 'publish_date':
            continue
        tmp_text = handling_string(child.text) 
        for t in tmp_text: 
            dis[t].add((tmp_id, child.tag))
# print(dis)


#root
for key in dis:

    tmp_main = ET.Element('keyword')
    # print(type(tmp_main)) 
    tmp_main.attrib = {'value':key} 

    for sub in dis[key]:
        tmp_book = ET.Element('book')     
        tmp_book.attrib = {'id': sub[0], 'attr':sub[1]}  
        tmp_main.append(tmp_book)
    root.append(tmp_main)
# print(root)

dom = xml.dom.minidom.parseString(ET.tostring(root))
pretty_xml_as_string = dom.toprettyxml()
# print(pretty_xml_as_string)

fp = open(output_name, 'w')
fp.write(pretty_xml_as_string)
fp.close()
