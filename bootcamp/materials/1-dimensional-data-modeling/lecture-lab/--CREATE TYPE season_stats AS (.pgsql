--CREATE TYPE season_stats AS (
--    season INTEGER,
--    gp REAL,
--    pts REAL,
--    reb REAL,
--    ast REAL
--);

--CREATE TYPE scoring_class AS ENUM( 'star','good','average','bad');

--CREATE TABLE players (
--    player_name TEXT,
--    height TEXT,
--    college TEXT,
--    country TEXT,
--    draft_year TEXT,
--    draft_round TEXT,
--    draft_number TEXT,
--    season_stats season_stats[],
--    scoring_class scoring_class,
--    years_since_last_season INTEGER,
--    current_season INTEGER,
--    is_active BOOLEAN,
--    PRIMARY KEY(player_name,current_season))

--CREATE TABLE players_scd (
--    player_name TEXT,
--    scoring_class scoring_class,
--    is_active BOOLEAN,
--    current_season INTEGER,
--    start_season INTEGER,
--    end_season INTEGER,
--    PRIMARY KEY(player_name, current_season)
--)

--DELETE FROM players;

--INSERT INTO players
--WITH yesterday AS (
--    SELECT * FROM players 
--    WHERE current_season = 1995
--),
--    today AS (
--        SELECT * FROM player_seasons
--        WHERE season = 1996
--    )
--
--    SELECT 
--        COALESCE(t.player_name,y.player_name),
--        COALESCE(t.height,y.height),
--        COALESCE(t.college,y.college),
--        COALESCE(t.country,y.country),
--        COALESCE(t.draft_year,y.draft_year),
--        COALESCE(t.draft_round, y.draft_round),
--        COALESCE(t.draft_number,y.draft_number),
--        CASE 
--            WHEN y.season_stats IS NULL
--                THEN ARRAY[ROW(
--                    t.season,
--                    t.gp,
--                    t.pts,
--                    t.reb,
--                    t.ast
--                    )::season_stats]
--            WHEN t.season IS NOT NULL
--                THEN  y.season_stats || ARRAY[ROW(
--                                                t.season,
--                                                t.gp,
--                                                t.pts,
--                                                t.reb,
--                                                t.ast
--                                                )::season_stats]
--            ELSE y.season_stats
--        END as season_stats,
--        CASE
--            WHEN t.season IS NOT NULL THEN
--                CASE 
--                    WHEN t.pts>20 THEN 'star'
--                    WHEN t.pts>15 THEN 'good'
--                    WHEN t.pts>10 THEN 'average'
--                    ELSE 'bad'
--                END::scoring_class
--            ELSE y.scoring_class
--        END, 
--        CASE
--            WHEN t.season IS NOT NULL THEN 0
--            ELSE COALESCE(y.years_since_last_season, 0) +1
--        END AS years_since_last_season,
--        COALESCE(t.season, y.current_season + 1) as current_season,
--        is_active
--
--    
--    FROM today t FULL OUTER JOIN yesterday y
--         ON t.player_name=y.player_name;
--
----SELECT player_name,scoring_class, is_active
--FROM players
--WHERE current_season =2022;


--INSERT INTO players_scd
WITH from_previous AS (SELECT 
    player_name,
    current_season,
    scoring_class,
    is_active,
    LAG(scoring_class,1) OVER(PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
    LAG(is_active,1) OVER(PARTITION BY player_name ORDER BY current_season) AS previous_is_active
FROM players
WHERE current_season<=2021),
    with_indicators AS (
            SELECT *,
            CASE
                WHEN scoring_class <> previous_scoring_class THEN 1
                WHEN is_active <> previous_is_active THEN 1
                ELSE 0
            END AS change_indicator
            FROM from_previous
    ),
    with_streak AS (
            SELECT *, 
                SUM(change_indicator) OVER(PARTITION BY player_name ORDER BY current_season) AS streak_identifier            FROM with_indicators
    )

SELECT 
    player_name,
    scoring_class,
    is_active,
    2021 AS current_season,
    MIN(current_season) AS start_season,
    MAX(current_season) AS end_season
FROM with_streak
GROUP BY player_name, streak_identifier, is_active, scoring_class
ORDER BY player_name, start_season ;

SELECT * FROM players_scd;