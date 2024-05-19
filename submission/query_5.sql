-- This query does the Incremental Load - one year at a time.
-- Insert data into the 'actors_history_scd' table
INSERT INTO actors_history_scd
WITH 
    -- Retrieve existing records from actors history table
    old_scd AS (
        SELECT * FROM actors_history_scd
    ),
    -- Fetch current year data for actors from the actors table
    current_year AS (
        SELECT * FROM actors WHERE current_year = 2016
    ),
    -- Combine previous and current year data to identify changes
    combined AS (
        SELECT
            COALESCE(ly.actor, ty.actor) AS actor,
            ly.is_active AS is_active_last_year,
            ty.is_active AS is_active_this_year,
            CASE
                WHEN ly.is_active <> ty.is_active OR ly.quality_class <> ty.quality_class THEN 1
                ELSE 0
            END AS did_change,
            ly.start_date AS start_date_last_year,
            ly.end_date AS end_date_last_year,
            ly.quality_class AS quality_class_last_year,
            ty.quality_class AS quality_class_this_year,
            ty.current_year AS current_year
        FROM old_scd ly
        FULL OUTER JOIN current_year ty 
        ON ly.actor = ty.actor AND ly.end_date + 1 = ty.current_year
    ),
    -- Determine changes to either initiate a new record or continue with an existing one
    changes AS (
        SELECT
            actor,
            did_change,
            CASE
                WHEN did_change = 0 THEN ARRAY[CAST(ROW(quality_class_last_year, is_active_last_year, start_date_last_year, end_date_last_year + 1) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))]
                WHEN did_change = 1 THEN ARRAY[
                    CAST(ROW(quality_class_last_year, is_active_last_year, start_date_last_year, end_date_last_year) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER)),
                    CAST(ROW(quality_class_this_year, is_active_this_year, current_year, current_year) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
                ]
                ELSE ARRAY[
                    CAST(ROW(COALESCE(quality_class_last_year, quality_class_this_year), COALESCE(is_active_last_year, is_active_this_year), start_date_last_year, end_date_last_year) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
                ]
            END AS change_array
        FROM combined
    ),
    -- Select the final attributes for the insertion.
    final AS (
        SELECT
            actor,
            arr.quality_class,
            arr.is_active,
            arr.start_date,
            arr.end_date
        FROM changes
        CROSS JOIN UNNEST(change_array) AS arr (quality_class, is_active, start_date, end_date)
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
