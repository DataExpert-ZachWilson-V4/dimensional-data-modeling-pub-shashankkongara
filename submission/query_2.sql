-- This query populates data for 'actors' table for one year at a time.
-- Inserting data into the 'actors' table
INSERT INTO actors
-- Subquery to get last year's data for actors
WITH last_year_data AS (
    SELECT
        actor,
        actor_id,
        films,
        quality_class,
        is_active,
        current_year
    FROM
        actors
    WHERE
        current_year = 2016
),
-- Subquery to fetch current year films data
current_year_films AS (
    SELECT
        actor,
        actor_id,
        film,
        votes,
        rating,
        film_id,
        year
    FROM
        bootcamp.actor_films
    WHERE
        year = 2017
),
-- Subquery to aggregate current year data and classify quality
current_year_agg AS (
    SELECT
        actor,
        actor_id,
        ARRAY_AGG(ROW(film, votes, rating, film_id)) AS films,
        CASE
            WHEN AVG(rating) <= 6 THEN 'bad'
            WHEN AVG(rating) > 6 AND AVG(rating) <= 7 THEN 'average'
            WHEN AVG(rating) > 7 AND AVG(rating) <= 8 THEN 'good'
            ELSE 'star'
        END AS quality_class,
        COUNT(DISTINCT film) > 0 AS is_active,
        MAX(year) AS current_year
    FROM
        current_year_films
    GROUP BY
        actor, actor_id
)
-- Final SELECT to combine data from last and current year and handle various cases
SELECT
    COALESCE(ly.actor, cy.actor) AS actor,
    COALESCE(ly.actor_id, cy.actor_id) AS actor_id,
    CASE
        WHEN cy.films IS NULL THEN ly.films
        WHEN ly.films IS NULL THEN cy.films
        ELSE ly.films || cy.films
    END AS films,
    COALESCE(cy.quality_class, ly.quality_class) AS quality_class,
    (cy.actor IS NOT NULL) AS is_active,
    2017 AS current_year
FROM
    current_year_agg cy
    FULL OUTER JOIN last_year_data ly
    ON cy.actor_id = ly.actor_id AND cy.actor = ly.actor
