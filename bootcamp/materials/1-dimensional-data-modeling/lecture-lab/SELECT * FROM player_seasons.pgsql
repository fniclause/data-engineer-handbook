SELECT * FROM player_seasons
ORDER BY player_name, season;


CREATE TYPE season_stats_ AS (
    gp REAL,
    pts REAL,
    reb REAL,
    ast REAL
)

CREATE TABLE players_ (
    player_name TEXT,
    height TEXT,
    weight INTEGER,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    season_stats season_stats_[],
    current_season INTEGER
)


INSERT INTO players_
WITH today AS (SELECT *
FROM player_seasons
WHERE season= 1997),
yesterday AS (SELECT *
FROM players_
WHERE current_season= 1996)

SELECT 
COALESCE(t.player_name,y.player_name),
COALESCE(t.height,y.height),
COALESCE(t.weight,y.weight),
COALESCE(t.college,y.college),
COALESCE(t.country,y.country),
COALESCE(t.draft_year,y.draft_year),
CASE
    WHEN y.season_stats IS NULL THEN ARRAY[ROW(t.gp,t.pts,t.reb,t.ast)::season_stats_]
    WHEN t.player_name IS NOT NULL THEN y.season_stats || ARRAY[ROW(t.gp,t.pts,t.reb,t.ast)::season_stats_]
    ELSE y.season_stats
END AS season_stats,
COALESCE(t.season,y.current_season + 1)
FROM today t FULL OUTER JOIN yesterday y
ON t.player_name = y.player_name;

SELECT * FROM players_
WHERE current_season = 1997;