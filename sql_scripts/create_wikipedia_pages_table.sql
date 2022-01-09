DROP TABLE IF EXISTS wikipedia_pages;

CREATE TABLE wikipedia_pages (
    id                              SERIAL PRIMARY KEY,
    lxml_page                       VARCHAR[]
);
