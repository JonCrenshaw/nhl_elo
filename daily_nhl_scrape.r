# Load necessary packages
library(dplyr)
library(tidyverse)
library(rvest)
library(purrr)
library(RPostgreSQL)
library(lubridate)

setwd('D:/Campaign_Hack/NHL Model')


scrape_season_data <- function(url) {
  # Use tryCatch to handle the HTTP error 429 and retry after a delay
  tryCatch({
    Sys.sleep(2)  # wait 2 seconds before sending the request
    read_html(url) %>% 
      html_nodes("table") %>% 
      html_table(fill = T) %>% 
      lapply(., function(x) setNames(x, c('date', 'visitor', 'goals_v', 'home', 'goals_h', 'ot', 'attendance', 'LOG', 'notes'))) %>% 
      map(~ mutate(.x, season = str_extract(url, "\\d{4}")))
  })
}

# Read in the "last scrape date" file, or create it with a date a few years ago if it doesn't exist
if (as.integer(format(Sys.Date(), "%m")) >= 8) { # web page is broken out by season, so "NHL_2024_games.html..." contains 2023 fall games
  current_year <- as.integer(format(Sys.Date(), "%Y")) + 1
} 

season_urls <- map(current_year, ~ paste0('https://www.hockey-reference.com/leagues/NHL_', .x, '_games.html#games')) 

# Scrape the data for each season URL and store it in a list of dataframes
season_data <- map(season_urls, scrape_season_data) %>% 
  compact() %>%  # remove NULL values
  bind_rows(.id="playoff") %>% # convert the list of dataframes to a single dataframe, create id column to indicate regular/post-season
  mutate(is_playoffs = ifelse(playoff == "2", TRUE,FALSE)) %>% 
  select(-playoff) %>% 
  mutate(attendance = as.character(attendance)) %>% 
  mutate(attendance = as.integer(gsub(",", "", attendance))) %>%  # Remove thousands comma from attendance column
  mutate(date = lubridate::ymd(date)) %>% # convert to date
  filter(!is.na(goals_v), !is.na(goals_h)) # Drop any rows where goals_v AND goals_h = NA (one rescheduled game, then any scheduled but unplayed games)


# Save the last scrape date to the "last_scrape_date.txt" file
writeLines(format(max(season_data$date), "%Y-%m-%d"), "last_scrape_date.txt")

write.csv(season_data, 'season_data_update.csv', row.names = FALSE, na = "")