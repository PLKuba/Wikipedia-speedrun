import json
from lxml import etree
from io import BytesIO
import re
from datetime import datetime
import configparser
import logging
from psycopg2 import connect
import concurrent.futures
from concurrent.futures import CancelledError
from concurrent.futures import ThreadPoolExecutor
import functools
import itertools
import CONFIG.config


# total lines in wiki file
LINE_COUNT = 1_288_367_801
# wikipedia file path
WIKI_FILE_PATH = 'DATA/enwiki-20211220-pages-articles-multistream.xml'
# wikipedia test file path
TEST_FILE_PATH = 'DATA/test.xml'
# regex to match redirections in page eg. "[[xyz]]"
REDIRECTION_REGEX = r'\[\[[^\]^\[]{1,}\]\]'
# regex to match redirect title in page eg. "<redirect title="David Talbot" />\n"
REDIRECT_TITLE_REGEX = r'<redirection_title = ".{0,255}">'
# max number of threads
MAX_THREADS = 30


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


def insert_pages_to_db(cur, page_limit=None,):
    logging.warning(f"Inserting {page_limit} pages to DB")
    with open(file=WIKI_FILE_PATH, mode='r') as file:
        total_pages = 1

        count = 0

        # while count <= LINE_COUNT and (page_limit is None or total_pages <= page_limit):
        while count <= LINE_COUNT:
            try:
                line = file.readline()

                # skip blank lines
                if not bool(line.strip()):
                    count+=1

                    continue

                lines_arr = []

                # getting a page
                if '<page>' in line:
                    while '</page>' not in line:
                        lines_arr.append(line)

                        count += 1

                        line = file.readline()

                    lines_arr.append(line)

                    total_pages += 1
                else:
                    continue

                cur.execute(CONFIG.config.insert_pages, (
                    lines_arr,
                ))

                conn.commit()

            except (TypeError, IndexError, KeyError, AttributeError) as e:
                logging.exception(e)

    # print("Succesfully updated database")


# function to fetch wikipedia titles and database titles
def update_db_with_page_values(cur, page_lines):
    try:
        # making a iterparse_lxml object from array
        parser = etree.XMLParser()

        for line in page_lines:
            parser.feed(line)

        root = parser.close()

        iterparse_lxml = etree.iterparse(BytesIO(etree.tostring(root)))

        database_title = None

        wikipedia_title = None

        redirections = None

        title_to_match_redirections = None

        # detecting an article page and redirection page
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

                            title_to_match_redirections_f = lambda: [element.text.lower() for event, element in etree.iterparse(BytesIO(etree.tostring(root))) if element.tag.strip().lower() == 'title']

                            title_to_match_redirections = title_to_match_redirections_f()[0]

            except (TypeError, IndexError, KeyError, AttributeError, IndexError) as e:
                logging.exception(e)

        # print(title_to_match_redirections, redirections)
        # print(wikipedia_title, database_title)

        # inserting data into db
        if database_title is not None and wikipedia_title is not None:
            # print("wikipedia_title: {0}\ndatabase_title: [{1}, ...]\n".format(wikipedia_title, database_title))

            cur.execute(CONFIG.config.get_id_by_title_titles, (
                wikipedia_title,
                database_title,
            ))

            res = cur.fetchone()

            if res is None:
                cur.execute(CONFIG.config.insert_titles, (
                    database_title,
                    wikipedia_title,
                    wikipedia_title,
                ))

            else:
                id = res[0]

                cur.execute(CONFIG.config.update_tiles, (
                    id,
                    database_title,
                    wikipedia_title,
                ))

        if title_to_match_redirections is not None and redirections is not None:
            # print("title_to_match_redirections: {0}\nredirections: [{1}, ...]\n".format(title_to_match_redirections, redirections.split(',')[0]))

            cur.execute(CONFIG.config.get_id_by_title_redirections, (
                title_to_match_redirections,
            ))

            res = cur.fetchone()

            if res is None:
                cur.execute(CONFIG.config.insert_redirections, (
                    redirections,
                    title_to_match_redirections,
                ))

            else:
                id = res[0]

                cur.execute(CONFIG.config.update_redirections, (
                    id,
                    redirections,
                ))


    except (TypeError, IndexError, KeyError, AttributeError, IndexError) as e:
        logging.exception(e)


@measure_time
def main(cur):
    # how many pages to insert to db, if no argument or None passed, whole file will be inserted
    # pages_to_update = None

    # insert_pages_to_db(cur)

    # how many pages to analyse, is None there's no LIMIT
    pages_to_analyse = None
    """LIMIT x OFFSET y""" # is a range between (y, y+x]


    cur.execute(CONFIG.config.get_range_pages_from_db,(
        1000,
        0
    ))

    res = cur.fetchall()

    if pages_to_analyse is not None:
        cur.execute(CONFIG.config.get_pages_from_db, (
            pages_to_analyse,
        ))
    else:
        cur.execute(CONFIG.config.get_all_pages_from_db)

    res = cur.fetchall()

    # for item in res:
    #     update_db_with_page_values(cur=cur, page_lines=item[0])

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        try:
            logging.warning("Updating DB")
            for item in res:
                executor.submit(update_db_with_page_values, cur=cur, page_lines=item[0])
        except CancelledError:
            pass


if __name__ == "__main__":
    # establish db connection
    conn = connect(database=CONFIG.config.DATABASE, user=CONFIG.config.USER,
                   password=CONFIG.config.PASSWORD, host=CONFIG.config.HOST,
                   port=CONFIG.config.PORT)

    cur = conn.cursor()

    with open(file='sql_scripts/create_wikipedia_table.sql') as f:
        cur.execute(f.read())

    # with open(file='sql_scripts/create_wikipedia_pages_table.sql') as f:
    #     cur.execute(f.read())

    main(cur)

    conn.commit()
    conn.close()
