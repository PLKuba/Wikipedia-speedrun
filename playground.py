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
from CONFIG.config import DATABASE, PASSWORD, USER, PORT, HOST


# total lines in wiki file
LINE_COUNT = 1_288_367_801
# wikipedia file path
WIKI_FILE_PATH = 'DATA/enwiki-20211220-pages-articles-multistream.xml'
# wikipedia test file path
TEST_FILE_PATH = 'DATA/test.xml'
# regex to match redirections in page eg. "[[xyz]]"
REDIRECTION_REGEX = r'\[\[[^\]^\[]{1,}\]\]'

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

            get_id_by_title = """SELECT id FROM wikipedia WHERE title_to_match=%s OR title_to_match=%s"""

            insert_titles = """INSERT INTO wikipedia (database_title, wikipedia_title, title_to_match)
                                    VALUES(%s, %s, %s)"""

            update_tiles = """INSERT INTO wikipedia (id)
                                    VALUES(%s)
                                    ON CONFLICT (id)
                                        DO
                                            UPDATE SET
                                            database_title=%s,
                                            wikipedia_title=%s"""

            get_id_by_title = """SELECT id FROM wikipedia WHERE title_to_match=%s"""

            insert_redirections = """INSERT INTO wikipedia (redirections, title_to_match)
                                        VALUES(%s, %s)"""

            update_redirections = """INSERT INTO wikipedia (id)
                                        VALUES(%s)
                                        ON CONFLICT (id)
                                            DO
                                                UPDATE SET
                                                redirections=%s"""

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
                    # print("database_title: {0}\nwikipedia_title: {1}\n".format(database_title, wikipedia_title))

                    cur.execute(get_id_by_title, (wikipedia_title, database_title))

                    res = cur.fetchone()

                    if res is None:
                        cur.execute(insert_titles, (
                            database_title,
                            wikipedia_title,
                            wikipedia_title,
                        ))

                    else:
                        id = res[0]
                        # print('ID: ', id)

                        cur.execute(update_tiles, (
                            id,
                            database_title,
                            wikipedia_title,
                        ))

                    continue

                if title_to_match_redirections is not None and redirections is not None:
                    # print("title_to_match_redirections: {0}\nredirections: [{1}, ...]\n".format(title_to_match_redirections, redirections.split(',')[0]))

                    cur.execute(get_id_by_title, (
                        title_to_match_redirections,
                    ))

                    res = cur.fetchone()

                    if res is None:
                        cur.execute(insert_redirections, (
                            redirections,
                            title_to_match_redirections,
                        ))

                    else:
                        id = res[0]
                        # print('ID: ', id)

                        cur.execute(update_redirections, (
                            id,
                            redirections,
                        ))

                    continue

        # print("Succesfully updated database")

    except (TypeError, IndexError, KeyError, AttributeError) as e:
        print(e)

    finally:
        if conn is not None:
            conn.close()


def main():
    fetch_wiki_titles_dbtitles()


if __name__ == "__main__":
    with open(file='sql_scripts/create_wikipedia_table.sql') as f:
        cur.execute(f.read())

    main()

    conn.commit()
    conn.close()