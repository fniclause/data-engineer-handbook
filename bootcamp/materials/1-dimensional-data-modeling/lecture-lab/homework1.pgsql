SELECT * FROM actor_films
limit 10;

-- 1. DDL actors 
CREATE TYPE films AS (
        film TEXT,
        votes INTEGER,
        rating REAL,
        filmid TEXT
);

CREATE TYPE quality_class AS ENUM('star','good','average','bad'); 

DROP TABLE actors;
CREATE TABLE actors (
    actor TEXT,
    year INTEGER,
    quality_class quality_class,
    films films[],
    is_active BOOLEAN
)

--2. Cumulative table generation query
INSERT INTO actors
SELECT 
    actor,
    year,
    CASE
        WHEN AVG(rating) > 8 THEN 'star'
        WHEN AVG(rating)>7 THEN 'good'
        WHEN AVG(rating)>6 THEN 'average'
        ELSE 'bad'
    END::quality_class AS AVG_rating,
    ARRAY_AGG(ROW(
                film,
                votes,
                rating,
                filmid
            )::films),
    TRUE AS is_active
FROM actor_films
GROUP BY actor,year;

--3. DDL for actors_history_scd

CREATE TABLE actors_history_scd (
    actor TEXT,
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER,
    start_year INTEGER,
    end_year INTEGER
)

-- 4. Backfill query for actors_history_scd
INSERT INTO actors_history_scd
WITH with_previous AS (
    SELECT
        actor,
        quality_class,
        is_active,
        year,
        LAG(quality_class,1) OVER (PARTITION BY actor ORDER BY year) AS previous_quality_class,
        LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY year) AS previous_is_active
    FROM actors
    WHERE year <= 2020
),
    with_indicators AS (
    SELECT *,
        CASE
            WHEN quality_class <> previous_quality_class THEN 1
            WHEN is_active<>previous_is_active THEN 1
            ELSE 0
        END AS change_indicator
    FROM with_previous
),
    with_streaks AS (
    SELECT *,
        SUM(change_indicator) OVER (PARTITION BY actor ORDER BY year) AS streak
    FROM with_indicators
)

SELECT 
    actor,
    quality_class,
    is_active,
    2020 AS current_year,
    MIN(year) AS start_year,
    MAX(year) AS end_year
    
FROM with_streaks
GROUP BY actor,streak, is_active, quality_class
;

--5. Incremental query for actors_history_scd

CREATE TYPE scd_type AS (
        quality_class quality_class,
        is_active BOOLEAN,
        start_year INTEGER,
        end_year INTEGER
)

INSERT INTO actors_history_scd
WITH last_year_scd AS (
    SELECT * FROM actors_history_scd
    WHERE end_year = 2020
),
    historical_scd AS (
        SELECT 
            actor,
            quality_class,
            is_active,
            start_year,
            end_year
        FROM actors_history_scd
        WHERE current_year = 2020
        AND end_year < 2020
),
    today AS (
        SELECT * FROM actors
        WHERE year = 2021
),
    unchanged_records AS (
        SELECT 
            td.actor,
            td.quality_class, 
            td.is_active,
            ls.start_year,
            td.year as end_year
        FROM today td
        JOIN last_year_scd ls
        ON td.actor = ls.actor
        WHERE td.quality_class = ls.quality_class
        AND td.is_active = ls.is_active
    ),
    changed_records AS (
        SELECT 
            td.actor,
            td.quality_class, 
            td.is_active,
            ls.start_year,
            td.year as end_year,
            UNNEST(ARRAY[
                ROW(
                    ls.quality_class,
                    ls.is_active,
                    ls.start_year,
                    ls.end_year

                )::scd_type,
                ROW(
                    td.quality_class,
                    td.is_active,
                    td.year,
                    td.year

                )::scd_type
            ]) AS records
        FROM today td
        JOIN last_year_scd ls
        ON td.actor = ls.actor
        WHERE (td.quality_class <> ls.quality_class
        OR td.is_active <> ls.is_active)
    ),
    unnested_change_records AS (
        SELECT actor,
            (records::scd_type).quality_class,
            (records::scd_type).is_active,
            (records::scd_type).start_year,
            (records::scd_type).end_year
         FROM changed_records
    ),
    new_records AS (
        SELECT 
            ts.actor,
            ts.quality_class,
            ts.is_active,
            ts.year,
            ts.year
        FROM today ts
        LEFT JOIN last_year_scd ls
            ON ts.actor = ls.actor
        WHERE ls.actor IS NULL
    )
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_change_records
UNION ALL
SELECT * FROM new_records;