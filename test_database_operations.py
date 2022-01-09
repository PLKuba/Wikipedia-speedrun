import psycopg2
import json
import configparser


# get config variables
config = configparser.ConfigParser()
config.read('CONFIG/test_pgadmin4.ini')

# configurate config variables
DATABASE = config["test_pgadmin4"]["database"]
PASSWORD = config["test_pgadmin4"]["password"]
USER = config["test_pgadmin4"]["user"]
PORT = config["test_pgadmin4"]["port"]
HOST = config["test_pgadmin4"]["host"]

conn = psycopg2.connect(database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT)

print("Database Connected....")

cur = conn.cursor()

with open(file="DATA/wiki_data.json") as f:
    page_json = json.load(f)
    # print(type(page_json))

redirections = json.dumps(page_json)

# cur.execute(sql , ('db_tytul', 'wiki_tytul'))
# cur.execute(get_id_by_title, ('dupa123', 'dupa456',))

sql = """SELECT redirections FROM wikipedia WHERE title_to_match = %s"""

cur.execute(sql, (
    'communications in afghanistan',
))

red_array = cur.fetchone()[0]

for item in red_array:
    sql = """SELECT redirections FROM wikipedia WHERE title_to_match = %s"""

    cur.execute(sql, (
        item,
    ))

    res = cur.fetchone()

    if res is not None:
        red_array = res[0]
        print(item)

        print(red_array, '\n')

# cur.execute(update_tiles, (id, 'dupa123', 'dupa456',))

# r = cur.fetchone()
# print(r)

conn.commit()
conn.close()

# print("Table Deleted....")
# print("Table Created....")
print("Table Updated....")

