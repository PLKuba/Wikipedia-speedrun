import psycopg2
from psycopg2 import connect
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

sql = """SELECT id FROM wikipedia WHERE wikipedia_title=%s"""

# sql = """INSERT INTO wikipedia (database_title, wikipedia_title)
#             VALUES(%s, %s)
#             ON CONFLICT (id)
#                 DO
#                     UPDATE SET
#                     redirections = %s"""

# cur.execute(sql , ('test_title1', 'test_title2', redirections))

cur.execute(sql , ('test_title2',))

r = cur.fetchone()
id = r[0]
print('Table id: ',id)
# r = cur.fetchone()
# print(r)

conn.commit()
conn.close()

# print("Table Deleted....")
# print("Table Created....")
print("Table Updated....")

