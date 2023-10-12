CREATE TEMP TABLE nhl_import(
    date DATE,
    visitor VARCHAR(250),
    goals_v INTEGER,
    home VARCHAR(250),
    goals_h INTEGER,
    ot VARCHAR(250),
    attendance INTEGER,
    log VARCHAR(250),
    notes VARCHAR(250),
    season INTEGER,
	is_playoffs BOOLEAN
);

COPY nhl_import 
FROM 'D:/Campaign_Hack/NHL Model/season_data_update.csv'
DELIMITER ',' CSV HEADER;

INSERT INTO season_data(date, visitor, goals_v, home, goals_h, ot, attendance, log, notes, season, is_playoffs)
SELECT date, visitor, goals_v, home, goals_h, ot, attendance, log, notes, season, is_playoffs
FROM nhl_import
WHERE (nhl_import.date, nhl_import.visitor, nhl_import.home) NOT IN (
    SELECT date, visitor, home FROM season_data
);

DROP TABLE nhl_import;