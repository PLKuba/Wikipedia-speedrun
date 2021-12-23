from datetime import datetime
import re
from lxml import __version__
print(__version__)

REDIRECT_REGEX = r'<redirect title=".+" \/>'

# xml_object - xml div, title or anything that has <xyz></xyz> structure.
# element - element to get from xml, like <title></title>, <id></id> or anything else.
# element is simply name of object eg. title, id, a, redirect.
def get_element_content_from_xml(xml_object: str, element: str) -> str:
    first_elem_index = xml_object.index(f'<{element}')

    last_elem_index = xml_object.index(f'</{element}>')

    return xml_object[(first_elem_index + len(f'<{element}>')):last_elem_index]


def find_redirect(xml_object: str)->str:


    return object


print("START\n")
start = datetime.now()
# with open(file="../../PycharmProjects/Wiki_speedrun/enwiki-20211201-pages-articles-multistream.xml", mode='r') as file:
#     content = file.readline()
#     # print(content)
#     while content is not None:
#         content = file.readline()

file = open("DATA/enwiki-20211220-pages-articles-multistream.xml", "r")
file2 = open("DATA/test.xml")
line_count = 0
page_count = 0
# print(file2.encoding)
# print(file2.readlines())

page = ""
in_page = False
for line in file:
    # print(line)
    line_count += 1
    if '<page>' in line:
        in_page = True

        page = ""

    elif '</page>' in line:
        page+=line
        page_count += 1

        in_page = False

        print(page)
        print('\n')
        print(get_element_content_from_xml(page, 'text'))

        print(re.findall(REDIRECT_REGEX, page))

        break

    if in_page:
        page+=line

# print(page)

file.close()
file2.close()
print("\nLines: {0}".format(line_count))
print("Pages: {0}".format(page_count))


elapsed = (datetime.now() - start).total_seconds()
print(elapsed)
print("DONE")

# wikipedia is in format <page>page content</page>
