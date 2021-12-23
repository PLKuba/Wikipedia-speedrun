import pprint
from lxml import etree
from io import BytesIO
import xml.dom.minidom
import time
import re
from datetime import datetime
import sre_constants
import constant


WIKI_FILE_PATH = 'DATA/enwiki-20211220-pages-articles-multistream.xml'
TEST_FILE_PATH = 'DATA/test.xml'
REDIRECTION_REGEX = r'\[\[[^\]^\[]{1,}\]\]'


# with open(file=WIKI_FILE_PATH, mode='rb') as file:
#     tree = etree.parse(file)
#
#     root = tree.getroot()
#
#     print(root.tag)
#
#     # print(etree.tostring(tree))


"""Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
def read_in_chunks(file_object, chunk_size=4096):
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data

print("START")
start = datetime.now()
with open(file=WIKI_FILE_PATH, mode='r') as file:
    count = 0

    parser = etree.XMLParser()

    for line in file:
        count+=1

        parser.feed(line)

    root = parser.close()

# for event, element in etree.iterparse(BytesIO(etree.tostring(root))):
#     print('%s, %4s, %s' % (event, element.tag, element.text))
redirection_dict = dict()
for element in root.iter():
    # print('%s - %s' % (element.tag, element.text))
    if element.tag == 'title':
        title = element.text
        # print(title)
    elif element.tag == 'text':
        if  '#REDIRECT' in element.text:
            article_self_redirect = re.findall(REDIRECTION_REGEX, element.text)
            redirection_dict[title] = {}
            redirection_dict[title]['self_redirect'] = article_self_redirect
        else:
            redirections = re.findall(REDIRECTION_REGEX, element.text)
            # print(redirections)s
            redirection_dict[title] = redirections
pprint.pp(redirection_dict)
# break
end_time = datetime.now()
total_execution_time = (end_time - start).total_seconds()
print(f'DONE in {total_execution_time} seconds')