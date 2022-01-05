CREATE TABLE wikipedia (
    id                  NOT NULL PRIMARY KEY AUTO_INCREMENT,
    database_title      varchar(255),
    wikipedia_title     varchar(255),
    redirections        JSON
);
