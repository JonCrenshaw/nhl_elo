DELETE FROM playoff_dates;

COPY playoff_dates
FROM 'D:/Campaign_Hack/NHL Model/elo project/playoff_dates.csv'
DELIMITER ',' CSV HEADER;

UPDATE season_data
SET is_playoffs = CASE WHEN season_data.date BETWEEN playoff_dates.start_date AND playoff_dates.end_date THEN TRUE ELSE FALSE END
FROM playoff_dates
WHERE season_data.season = playoff_dates.season;
