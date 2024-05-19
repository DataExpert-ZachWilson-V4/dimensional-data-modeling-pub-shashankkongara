-- Creating Type 2 Slowly Changing Dimension Table - 'actors_history_scd' table
CREATE OR REPLACE TABLE actors_history_scd(
    actor VARCHAR,           -- Column to store actor name
    quality_class VARCHAR,   -- Column to specify categorical rating based on average rating in the most recent year
    is_active BOOLEAN,       -- Boolean column to indicate if the actor is currently active in making films in current year
    start_date INTEGER,      -- Represents the Start date of the actor's State - active/inactive period - crucial in storing history for the type 2 SCD table
    end_date INTEGER,        -- Represents the End date of the actor's State - active/inactive period - crucial in storing history for the type 2 SCD table
    current_year INTEGER     -- Current year
)
WITH
(
    FORMAT = 'PARQUET',                    -- Storing in Parquet Format for efficiency
    partitioning = ARRAY['current_year']   -- Partitioning the table by 'current_year' for optimized queries and quick data retrieval
)
