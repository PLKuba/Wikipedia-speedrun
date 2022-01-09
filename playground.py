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
from CONFIG.config import DATABASE, PASSWORD, USER, PORT, HOST,\
get_id_by_title_titles, insert_titles,update_tiles, get_id_by_title_redirections,\
insert_redirections, update_redirections, insert_pages


# total lines in wiki file
LINE_COUNT = 1_288_367_801
# wikipedia file path
WIKI_FILE_PATH = 'DATA/enwiki-20211220-pages-articles-multistream.xml'
# wikipedia test file path
TEST_FILE_PATH = 'DATA/test.xml'
# regex to match redirections in page eg. "[[xyz]]"
REDIRECTION_REGEX = r'\[\[[^\]^\[]{1,}\]\]'

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


@measure_time
def insert_pages_to_db(page_limit=10):
    try:
        with open(file=WIKI_FILE_PATH, mode='r') as file:
            total_pages = 1

            count = 0

            while count <= LINE_COUNT and total_pages <= page_limit:
                line = file.readline()

                # parser = etree.XMLParser()

                if not bool(line.strip()):
                    count+=1

                    continue

                if '<page>' in line:
                    lines_arr = []

                    while '</page>' not in line:
                        # parser.feed(line)
                        lines_arr.append(line)

                        count += 1

                        line = file.readline()

                    # parser.feed(line)
                    lines_arr.append(line)

                    total_pages += 1

                else:
                    continue
                # print(str_to_pass)
                cur.execute(insert_pages, (lines_arr,))

                # root = parser.close()

                # root_string = etree.tostring(root)

                # print(etree.tostring(root).decode('utf-8'))
    except (TypeError, IndexError, KeyError, AttributeError) as e:
        logging.exception(e)

    finally:
        # if conn.closed:
        #     conn.close()
        pass


# function to fetch wikipedia titles and database titles
@measure_time
def update_db_with_page_values(page_lines):
    try:
        count = 0

        database_title = None

        wikipedia_title = None

        redirections = None

        title_to_match_redirections = None

        parser = etree.XMLParser()

        for line in page_lines:
            parser.feed(line)

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

                            title_to_match_redirections_f = lambda : [element.text for event, element in iterparse_lxml if element.tag.strip() == 'title']

                            title_to_match_redirections = title_to_match_redirections_f()[0].lower()

            except (TypeError, IndexError, KeyError, AttributeError) as e:
                logging.exception(e)
                print(count)

        if database_title is not None and wikipedia_title is not None:
            # print("database_title: {0}\nwikipedia_title: {1}\n".format(database_title, wikipedia_title))

            cur.execute(get_id_by_title_titles, (
                wikipedia_title,
                database_title,
            ))

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

            # conn.commit()
            # conn.close()

            return

        if title_to_match_redirections is not None and redirections is not None:
            # print("title_to_match_redirections: {0}\nredirections: [{1}, ...]\n".format(title_to_match_redirections, redirections.split(',')[0]))

            cur.execute(get_id_by_title_redirections, (
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

            # conn.commit()
            # conn.close()

            return

        print("Succesfully updated database")

    except (TypeError, IndexError, KeyError, AttributeError) as e:
        logging.exception(e)

    finally:
        # if conn.closed:
        #     conn.close()
        pass


if __name__ == "__main__":
    # establish db connection
    conn = connect(database=DATABASE, user=USER,
                   password=PASSWORD, host=HOST,
                   port=PORT)

    cur = conn.cursor()

    with open(file='sql_scripts/create_wikipedia_table.sql') as f:
        cur.execute(f.read())

    with open(file='sql_scripts/create_wikipedia_pages_table.sql') as f:
        cur.execute(f.read())

    # how many pages to insert to db
    insert_pages_to_db(page_limit=10)

    get_page_from_db = """SELECT lxml_page FROM wikipedia_pages LIMIT %s"""

    pages_to_fetch = 1

    cur.execute(get_page_from_db, (
        pages_to_fetch
    ))

    res = cur.fetchall()

    update_db_with_page_values(res[0][0])

    conn.commit()
    conn.close()


# TODO: NEW PLAN ON MULTITHREADING THIS THINGS
# TODO: DONE       # WRITE ALL PAGES TO DATABASE (THIS WILL BE ARRAY LIKE THING) (CREATE SEPERATE TABLE)
# TODO: DONE       # CREATE TABLE WITH USER DEFINED TYPES
# TODO: MULTITHREAD THIS THING :)