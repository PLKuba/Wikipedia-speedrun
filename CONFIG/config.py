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


get_id_by_title_titles = """SELECT id FROM wikipedia WHERE title_to_match=%s OR title_to_match=%s"""

insert_titles = """INSERT INTO wikipedia (database_title, wikipedia_title, title_to_match)
                        VALUES(%s, %s, %s)"""

update_tiles = """INSERT INTO wikipedia (id)
                        VALUES(%s)
                        ON CONFLICT (id)
                            DO
                                UPDATE SET
                                database_title=%s,
                                wikipedia_title=%s"""

get_id_by_title_redirections = """SELECT id FROM wikipedia WHERE title_to_match=%s"""

insert_redirections = """INSERT INTO wikipedia (redirections, title_to_match)
                            VALUES(%s, %s)"""

update_redirections = """INSERT INTO wikipedia (id)
                            VALUES(%s)
                            ON CONFLICT (id)
                                DO
                                    UPDATE SET
                                    redirections=%s"""

insert_pages = "INSERT INTO wikipedia_pages (lxml_page) VALUES (%s)"