-- This query does `backfill` of inserting data into SCD table for all the years at once.
-- Inserting data into the 'actors_history_scd' table

INSERT INTO actors_history_scd
WITH
    -- Calculate values and their lag from the previous year for comparison
    lagged AS (
        SELECT
            actor,
            CASE WHEN is_active THEN 1 ELSE 0 END AS is_active,
            CASE
                WHEN lag(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) THEN 1
                ELSE 0
            END as is_active_last_year,
            quality_class,
            lag(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) AS quality_class_last_year,
            current_year
        FROM
            actors
    ),
    -- Compute a streak_identifier to track changes and continuity in actor activity over years
    streaked AS (
        SELECT
            *,
            CASE
                WHEN is_active <> is_active_last_year OR quality_class <> quality_class_last_year THEN 1
                ELSE 0
            END AS did_change,
            SUM(
                CASE
                    WHEN is_active <> is_active_last_year OR quality_class <> quality_class_last_year THEN 1
                    ELSE 0
                END
            ) OVER (
                PARTITION BY actor
                ORDER BY current_year
            ) AS streak_identifier
        FROM
            lagged
    ),
    -- Group by streak identifier to consolidate periods of consistent activity or quality changes
    final AS (
        SELECT
            actor,
            quality_class,
            CASE WHEN MAX(is_active) = 1 THEN TRUE ELSE FALSE END AS is_active, -- Setting is_active based on the streak
            MIN(current_year) AS start_date, -- The start year of the current streak
            MAX(current_year) AS end_date -- The end year of the current streak
        FROM
            streaked
        GROUP BY
            actor, streak_identifier, quality_class
    ),
    -- Fetch the maximum current year from the actors table
    max_year AS (
        SELECT MAX(current_year) as current_year FROM actors
    )
-- Select all final results and cross join with the max year to include the most recent year in the output
SELECT
    *
FROM
    final a
    CROSS JOIN max_year b
ORDER BY 
    a.actor
