import json
import os
from lxml import etree
from io import BytesIO
import re
from datetime import datetime
import configparser
import logging
from psycopg2 import connect
import concurrent.futures
from concurrent.futures import CancelledError, TimeoutError
from concurrent.futures import ThreadPoolExecutor
import functools
import itertools
import CONFIG.config


# shell command to read lines in a file (sed gives an exact result unlike 'wc')
READ_LINES_COMMAND = "sed -n '$=' DATA/test.xml"
# total lines in wiki file
LINE_COUNT = 1_288_367_801
# wikipedia file path
WIKI_FILE_PATH = 'DATA/enwiki-20211220-pages-articles-multistream.xml'
# wikipedia test file path
TEST_FILE_PATH = 'DATA/test.xml'
# regex to match the following: [[xyz]]
REDIRECTION_REGEX = r'\[\[[^\]^\[]{1,}\]\]'
# regex to match the following: <redirect title="David Talbot" />\n
REDIRECT_TITLE_REGEX = r'<redirect title=".{0,255}">'
# regex to match the following: <title>AccessibleComputing</title>
TITLE_REGEX = r''
# regex to match the following: <text bytes="111" xml:space="preserve">#REDIRECT [[Computer accessibility]]
REDIRECT_REGEX = r''
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


# function to update database with all wikipedia content
def insert_pages_to_db(cur, page_limit=None, count_lines=False):
    logging.warning(f"Inserting {page_limit} pages to DB")

    with open(file=WIKI_FILE_PATH, mode='r') as file:
        total_pages = 1

        count = 0

        # while count <= LINE_COUNT and (page_limit is None or total_pages <= page_limit):

        if count_lines:
            return_value = os.popen(READ_LINES_COMMAND).read()
            LINE_COUNT = int(return_value[:-1])

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
                # conn.close()

            except (TypeError, IndexError, KeyError, AttributeError) as e:
                logging.exception(e)

    print("Succesfully updated database")


def parse_redirections(redirections):
    redirections2 = {
        'links':[],
        'files': [],
        'categories': [],
        'wikipedia_exclusive': [],
        'count':len(redirections)
    }

    for item in redirections:
        if item.startswith('category:'):
            val1 = item[len('category:'):]

            # redirections2.append((val1, None))
            redirections2["categories"].append(val1)
            redirections2["links"].append(val1)

        if item.startswith(':category:'):
            val1 = item[len(':category:'):]

            redirections2["categories"].append(val1)
            redirections2["links"].append(val1)

        elif '|' in item:
            val1 = item[item.index('|') + 1:]
            val2 = item[:item.index('|')]

            # redirections2.append((val1, val2))
            redirections2['links'].append(val1)

        elif item.startswith('wikipedia:'):
            redirections2["wikipedia_exclusive"].append(item)

        elif item.startswith('file:'):
            redirections2["files"].append(item)

        else:
            redirections2['links'].append(item)

    return redirections2
    # return json.dumps(redirections2)


# function to fetch wikipedia titles and database titles
def update_db_with_page_content(cur, page_lines):
    titles = []

    try:
        # check if this is just a redirect page
        for item in page_lines:
            try:
                # Titles:
                # <title>AccessibleComputing</title>
                # titles.append(re.search(TITLE_REGEX, item).string[17:-4])

                # <redirect title="Computer accessibility" />
                content = re.search(REDIRECT_TITLE_REGEX, item).string[17:-4]
                # print(content)
                titles.append(content)

                # <text bytes="111" xml:space="preserve">#REDIRECT [[Computer accessibility]]
                # titles.append(re.search(REDIRECT_REGEX, item).string[17:-4])

                # TODO: find all titles and store them in an array or smth, there should not be only one source or a title like string
                # Variety of those is too much

                # return (title_to_match_redirections, title_to_match_redirections, None, None)
            except (TypeError, IndexError, KeyError, AttributeError):
                pass

        # making a iterparse_lxml object from array
        parser = etree.XMLParser()

        for line in page_lines:
            parser.feed(line)

        root = parser.close()

        iterparse_lxml = etree.iterparse(BytesIO(etree.tostring(root)))

        database_title = None

        wikipedia_title = None

        redirections_json = None

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
                            redirections = [redirection[2:-2].lower() for redirection in match]

                            redirections = parse_redirections(redirections)

                            # print(redirections)

                            redirections_json = redirections

                            title_to_match_redirections_f = lambda: [element.text.lower() for event, element in etree.iterparse(BytesIO(etree.tostring(root))) if element.tag.strip().lower() == 'title']

                            title_to_match_redirections = title_to_match_redirections_f()[0]

            except (TypeError, IndexError, KeyError, AttributeError, IndexError) as e:
                logging.exception(e)

        # print(title_to_match_redirections, redirections_json)
        # print(wikipedia_title, database_title)

        # print(database_title, wikipedia_title, title_to_match_redirections, redirections_json)
        # inserting data into db
        if database_title is not None and wikipedia_title is not None:
            print("wikipedia_title: {0}\ndatabase_title: [{1}, ...]\n".format(wikipedia_title, database_title))

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

        if title_to_match_redirections is not None and redirections_json is not None:
            print("title_to_match_redirections: {0}\nredirections_json: [{1}, ...]\n".format(title_to_match_redirections, json.dumps(redirections_json, indent=4)))

            cur.execute(CONFIG.config.get_id_by_title_redirections, (
                title_to_match_redirections,
            ))

            res = cur.fetchone()

            if res is None:
                cur.execute(CONFIG.config.insert_redirections, (
                    redirections_json,
                    title_to_match_redirections,
                ))

            else:
                id = res[0]

                cur.execute(CONFIG.config.update_redirections, (
                    id,
                    redirections_json,
                ))

        conn.commit()

        return (title_to_match_redirections, wikipedia_title, database_title, redirections_json)

    except (TypeError, IndexError, KeyError, AttributeError, IndexError) as e:
        logging.exception(e)


@measure_time
def main(cur, step=2, start_id=0, LIMIT=LINE_COUNT):
    try:
        if step is not None:
            cur.execute(CONFIG.config.get_range_pages_from_db, (
                        step,
                        start_id,
            ))
            print(start_id)
            res = cur.fetchall()
            # print(res)
            # return

            start_id += step

            futures = []

            # while bool(res) and start_id<=LIMIT:
            while bool(res):
                print(start_id)
                with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                    try:
                        # logging.warning("Updating DB")

                        for item in res:
                            # print(item)
                            # print("id: ", item[0])
                            futures.append(executor.submit(update_db_with_page_content, cur=cur, page_lines=item[1]))
                            # break

                        # for future in concurrent.futures.as_completed(futures):
                        #     print(future.result())
                        # print(futures)

                        # this break is to only fetch one page!!!!
                        # conn.close()
                        # break

                    except (CancelledError, TimeoutError):
                        logging.warning('Cancelled or Timeout Error')
                        pass

                # 36894 error occurs here
                cur.execute(CONFIG.config.get_range_pages_from_db, (
                    step,
                    start_id,
                ))

                res = cur.fetchall()

                start_id += step

    except (TypeError, IndexError, KeyError, AttributeError, IndexError):
        return None


if __name__ == "__main__":
    # establish db connection
    conn = connect(database=CONFIG.config.DATABASE, user=CONFIG.config.USER,
                   password=CONFIG.config.PASSWORD, host=CONFIG.config.HOST,
                   port=CONFIG.config.PORT)

    cur = conn.cursor()

    with open(file='sql_scripts/create_wikipedia_table.sql') as f:
        cur.execute(f.read())

    # if you want to update all wikipedia run this:
    # with open(file='sql_scripts/create_wikipedia_pages_table.sql') as f:
    #     cur.execute(f.read())
    # insert_pages_to_db(cur)


    # main(cur, LIMIT=10000)
    main(cur)

    conn.commit()
    conn.close()

# stats
# insert 10 - 0.03s
# insert 100 - 0.33s
# insert 1000 - 3.885619s
# insert 10000 - 36.372222
# insert 100000 -

