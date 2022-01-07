DROP TABLE IF EXISTS wikipedia;

CREATE TABLE wikipedia (
    id                  SERIAL PRIMARY KEY,
    database_title                  varchar(255),
    wikipedia_title                 varchar(255),
    redirections                    JSON,
    title_to_match_redirections     varchar(255)
);

----do $$
----declare
----  selected_film film%rowtype;
----  input_film_id film.film_id%type := 0;
----begin
----
----  select * from film
----  into selected_film
----  where film_id = input_film_id;
----
----  if not found then
----     raise notice'The film % could not be found',
----	    input_film_id;
----  end if;
----end $$
--
--do $$
--select redirections form wikipedia
--where redirections = %s
--
--end $$