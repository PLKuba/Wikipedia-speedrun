import psycopg2
from psycopg2 import connect
import json
import configparser


config = configparser.ConfigParser()

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
    print(type(page_json))

redirections = json.dumps(page_json)

# sql = """INSERT INTO wikipedia_test (database_title, wikipedia_title, redirections)
#                             VALUES(%s, %s, %s)"""

sql = """
CREATE TABLE wikipedia (
    id                  SERIAL PRIMARY KEY,
    database_title      varchar(255),
    wikipedia_title     varchar(255),
    redirections        JSON
);
"""

cur.execute(sql)

# r = cur.fetchall()
# print(r)

# print("Table Deleted....")
print("Table Created....")

conn.commit()
conn.close()