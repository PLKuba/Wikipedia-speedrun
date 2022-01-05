DROP TABLE IF EXISTS wikipedia;

CREATE TABLE wikipedia (
    id                  SERIAL PRIMARY KEY,
    database_title      varchar(255),
    wikipedia_title     varchar(255),
    redirections        JSON
);
