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
import itertools


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

            # TRY: insert on conflict
            insert_titles_sql = """INSERT INTO wikipedia (database_title, wikipedia_title)
                                       (SELECT %s, %s FROM wikipedia WHERE redirections = %s);"""

            insert_redirections_sql = """INSERT INTO wikipedia (redirections)
                                       (SELECT %s FROM wikipedia WHERE database_title = %s or database_title = %s);"""

            while count <= LINE_COUNT:
                line = file.readline()

                parser = etree.XMLParser()

                database_title = None

                wikipedia_title = None

                redirections = None

                title_to_match_redirections = None

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

                iterparse_lxml = etree.iterparse(BytesIO(etree.tostring(root)))

                for event, element in iterparse_lxml:
                    try:
                        if element.tag.strip() == 'title':
                            database_title = element.text.lower()

                        elif element.tag.strip() == 'text':
                            match = re.findall(REDIRECTION_REGEX, element.text.strip())

                            if match is not None:
                                if '#REDIRECT' in element.text.strip()[:10]:
                                    wikipedia_title = match[0][2:-2].lower()

                                    break

                                else:
                                    redirections = json.dumps([redirection[2:-2].lower() for redirection in match])

                                    title_to_match_redirections_f = lambda : [element.text for event, element in etree.iterparse(BytesIO(etree.tostring(root))) if element.tag.strip() == 'title']

                                    title_to_match_redirections = title_to_match_redirections_f()[0].lower()

                    except (TypeError, IndexError, KeyError, AttributeError) as e:
                        logging.warning(e)
                        print(count)

                if database_title is not None and wikipedia_title is not None:
                    print("database_title: {0}\nwikipedia_title: {1}\n".format(database_title, wikipedia_title))

                    cur.execute(insert_titles_sql, (
                        database_title,
                        wikipedia_title,
                        redirections
                    ))

                    continue

                if title_to_match_redirections is not None and redirections is not None:
                    print("title_to_match_redirections: {0}\nredirections: {1}\n".format(title_to_match_redirections, redirections))

                    cur.execute(insert_redirections_sql, (
                        redirections,
                        database_title,
                        wikipedia_title
                    ))

                    continue

        print("Succesfully updated database")

    except (TypeError, IndexError, KeyError, AttributeError) as e:
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