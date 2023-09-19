# Load necessary packages
library(dplyr)
library(tidyverse)
library(rvest)
library(purrr)
library(RPostgreSQL)
library(lubridate)

# Set the working directory to the folder where the data will be stored
setwd('D:/Campaign_Hack/NHL Model')


# Define a function to scrape the data from a single URL
scrape_season_data <- function(url) {
  # Use tryCatch to handle the HTTP error 429 and retry after a delay
  tryCatch({
    Sys.sleep(2)  # wait 2 seconds before sending the request
    read_html(url) %>% 
      html_nodes("table") %>% 
      html_table(fill = T) %>% 
      lapply(., function(x) setNames(x, c('date', 'visitor', 'goals_v', 'home', 'goals_h', 'ot', 'attendance', 'LOG', 'notes'))) %>% 
      map(~ mutate(.x, season = str_extract(url, "\\d{4}")))
  }, error = function(e) {
    message(paste0("Error occurred when scraping data from URL: ", url))
    NULL
  })
}

# Read in the "last scrape date" file, or create it with a date a few years ago if it doesn't exist
if (file.exists("last_scrape_date.txt")) {
  last_scrape_date <- as.Date(readLines("last_scrape_date.txt"), "%Y-%m-%d")
} else {
  last_scrape_date <- as.Date("1917-12-19")
}

# Define a list of URLs for each NHL season since the last scrape date
current_year <- as.integer(format(Sys.Date(), "%Y"))
current_month <- as.integer(format(Sys.Date(), "%m"))
if (current_month >= 8) { # web page is broken out by season, so "NHL_2024__games.html..." contains 2023 fall games
  current_year <- current_year + 1
} 
season_years <- seq(year(last_scrape_date) - 1, current_year, by = 1)
season_urls <- map(season_years, ~ paste0('https://www.hockey-reference.com/leagues/NHL_', .x, '_games.html#games'))

# Scrape the data for each season URL and store it in a list of dataframes
season_data_list <- map(season_urls, scrape_season_data) %>% 
  compact() %>%  # remove NULL values
  flatten()      # convert the list of dataframes to a single dataframe

# Remove thousands comma from attendance column
season_data_list <- lapply(season_data_list, function(x) {
  x %>%
    mutate(attendance = as.integer(gsub(",", "", attendance)))
})

# Use map to apply the lubridate::mdy() function to each date column in the season_data_list
season_data_list <- map(season_data_list, ~ mutate(.x, date = lubridate::ymd(date)))


# Combine all the dataframes in the list into one using dplyr::bind_rows
season_data <- bind_rows(season_data_list)

# Drop any rows where goals_v AND goals_h = NA (one rescheduled game, then any scheduled but unplayed games)
season_data <- season_data %>% 
  filter(!is.na(goals_v), !is.na(goals_h))

# Save the last scrape date to the "last_scrape_date.txt" file
writeLines(format(max(season_data$date), "%Y-%m-%d"), "last_scrape_date.txt")

write.csv(season_data, 'season_data_update.csv', row.names = FALSE, na = "")