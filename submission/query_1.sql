-- Creating the 'actors' table with a cumulative table design
CREATE OR REPLACE TABLE actors(
    actor VARCHAR, -- Column to store the actor's name
    actor_id VARCHAR, -- Column to store a unique identifier for the actor
    films ARRAY( -- Array of ROWs to store multiple films per actor - to track the history.
        ROW(
            film VARCHAR, -- Name of the film
            votes INTEGER, -- Number of votes the film received
            rating DOUBLE, -- Rating of the film
            film_id VARCHAR -- Unique identifier for the film
        )
    ),
    quality_class VARCHAR, -- Column to specify categorical rating based on rating in a year.
    is_active BOOLEAN, -- Boolean column to indicate if the actor is currently active in making films in a year
    current_year INTEGER -- Current year
)
WITH
(
    FORMAT = 'PARQUET', -- Storing in Parquet Format for efficiency
    partitioning = ARRAY['current_year'] -- -- Partitioning the table by 'current_year' for optimized queries and quick data retrieval
)
