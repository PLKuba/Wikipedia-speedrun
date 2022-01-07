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

"""redirections"""
# get id where redirections are inserted
get_id_by_redirections_title = """SELECT id FROM wikipedia WHERE title_to_match_redirections=%s"""

# insert redirections to db with titles in it
insert_redirections = """INSERT INTO wikipedia (redirections, title_to_match_redirections)
        VALUES(%s, %s)"""

update_redirections = """INSERT INTO wikipedia (id)
                                VALUES(%s)
                                ON CONFLICT (id)
                                    DO
                                        UPDATE SET
                                        redirections=%s,
                                        title_to_match_redirections=%s"""

"""titles"""
# get id where titles are inserted
get_id_by_title = """SELECT id FROM wikipedia WHERE wikipedia_title=%s OR wikipedia_title=%s"""

insert_titles = """INSERT INTO wikipedia (database_title, wikipedia_title)
        VALUES(%s, %s)"""

# insert titles to db with redirections in it
update_tiles = """INSERT INTO wikipedia (id)
                                VALUES(%s)
                                ON CONFLICT (id)
                                    DO
                                        UPDATE SET
                                        database_title=%s,
                                        wikipedia_title=%s"""

# cur.execute(sql , ('db_tytul', 'wiki_tytul'))
cur.execute(get_id_by_title, ('dupa123', 'dupa456',))

r = cur.fetchone()
print(r)
if r is None:
    print("LOL JESTES NONE")
id = r[0]
print(type(id))
print(bytes(str(id), encoding='utf-8'))

cur.execute(update_tiles, (id, 'dupa123', 'dupa456',))

# r = cur.fetchone()
# print(r)

conn.commit()
conn.close()

# print("Table Deleted....")
# print("Table Created....")
print("Table Updated....")

