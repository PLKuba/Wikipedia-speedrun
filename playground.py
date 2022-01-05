import json

from lxml import etree
from io import BytesIO
import re
from datetime import datetime
import configparser
import logging
from psycopg2 import connect
from concurrent.futures import CancelledError
from concurrent.futures import ThreadPoolExecutor
import functools


# total lines in wiki file
LINE_COUNT = 1_288_367_801
# wikipedia file path
WIKI_FILE_PATH = 'DATA/enwiki-20211220-pages-articles-multistream.xml'
# wikipedia test file path
TEST_FILE_PATH = 'DATA/test.xml'
# regex to match redirections in page eg. "[[xyz]]"
REDIRECTION_REGEX = r'\[\[[^\]^\[]{1,}\]\]'

# get config variables
config = configparser.ConfigParser()
config.read('CONFIG/test_pgadmin4.ini')

# configurate config variables
DATABASE = config["test_pgadmin4"]["database"]
PASSWORD = config["test_pgadmin4"]["password"]
USER = config["test_pgadmin4"]["user"]
PORT = config["test_pgadmin4"]["port"]
HOST = config["test_pgadmin4"]["host"]

# establish db connection
conn = connect(database=DATABASE, user=USER,
               password=PASSWORD, host=HOST,
               port=PORT)

cur = conn.cursor()


# decorator that measures the time
def measure_time(func):
    @functools.wraps(func)
    def dec_inner(*args, **kw):
        logging.warning("START")

        start = datetime.now()

        ret = func(*args, **kw)

        end_time = datetime.now()

        total_execution_time = (end_time - start).total_seconds()

        logging.warning(f'DONE in {total_execution_time} seconds')

        return ret
    return dec_inner


# function to fetch wikipedia titles and database titles
@measure_time
def fetch_wiki_titles_dbtitles():
    try:
        with open(file=WIKI_FILE_PATH, mode='r') as file:
            count = 0

            while count <= LINE_COUNT:
                line = file.readline()

                parser = etree.XMLParser()

                database_title = None

                wikipedia_title = None

                redirections = None

                if not bool(line.strip()):
                    count+=1

                    continue

                if '<page>' in line:
                    while '</page>' not in line:
                        parser.feed(line)

                        count += 1

                        line = file.readline()

                    parser.feed(line)

                else:
                    continue

                root = parser.close()

                for event, element in etree.iterparse(BytesIO(etree.tostring(root))):
                    try:
                        if element.tag.strip() == 'title':
                            database_title = element.text

                        elif element.tag.strip() == 'text':
                            if re.search(REDIRECTION_REGEX, element.text.strip()) \
                            and '#REDIRECT' in element.text.strip():
                                wikipedia_title = re.findall(REDIRECTION_REGEX, element.text.strip())[0][2:-2]

                                break

                            else:
                                redirections = re.findall(REDIRECTION_REGEX, element.text)

                                redirections = json.dumps([red[2:-2] for red in redirections])

                    except (TypeError, IndexError, AttributeError) as e:
                        logging.warning(e)
                        print(count)

                if database_title is None or wikipedia_title is None:
                    continue

                print("database_title: {0}\nwikipedia_title: {1}\n".format(database_title, wikipedia_title))

                sql = """INSERT INTO wikipedia (database_title, wikipedia_title)
                                VALUES(%s, %s)"""

                cur.execute(sql, (
                    database_title,
                    wikipedia_title
                ))

                print("Succesfully updated database")
                break

    except (TypeError, IndexError, AttributeError) as e:
        print(e)

    finally:
        # if conn is not None:
        #     conn.close()
        pass


def main():
    fetch_wiki_titles_dbtitles()


if __name__ == "__main__":
    with open(file='sql_scripts/create_wikipedia_table.sql') as f:
        cur.execute(f.read())

    main()

    conn.commit()
    conn.close()