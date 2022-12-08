# Helper functions

# Date handling for fiscal years ==========================

# Adapted from
# https://github.com/GoC-Spending/contracts-data/blob/main/lib/helpers.R#L103

# Generate the matching quarter ("Q3") from a given date ("2018-02-03")
get_quarter_from_date <- function(date) {
  
  # Quick check that the input date is valid:
  date <- ymd(date)
  
  # Months: 
  #  April to June = 04 to 06 = Q1
  #  July to September = 07 to 09 = Q2
  #  October to December = 10 to 12 = Q3
  #  January to March = 01 to 03 = Q4
  
  quarter <- case_when(
    month(date) <= 3 ~ "Q4",
    month(date) <= 6 ~ "Q1",
    month(date) <= 9 ~ "Q2",
    month(date) <= 12 ~ "Q3",
    TRUE ~ as.character(NA)
  )
  
  return(quarter)
}



# Generate the matching "short" fiscal year ("2021", the year the FY started in) 
# from a given date
get_short_fiscal_year_from_date <- function(date) {
  
  # Quick check that the input date is valid:
  date <- ymd(date)
  quarter <- get_quarter_from_date(date)
  
  year <- case_when(
    quarter == "Q4" ~ year(date) - 1, # FY started the previous year
    quarter %in% c("Q1", "Q2", "Q3") ~ year(date), # FY started this year
    TRUE ~ NA_real_
  )
  
  return(year)
  
}

# Create a typical fiscal year ("2021-2022") from a start year ("2021")
convert_start_year_to_fiscal_year <- function(start_year) {
  
  end_year <- as.integer(start_year) + 1
  return(str_c(start_year, "-", end_year))
  
}

# Take a fiscal year ("2021-2022")and output an integer start year (2021)
convert_fiscal_year_to_start_year <- function(fiscal_year) {
  
  # Extracts a 4-digit number; this works given the consistent reporting period format.
  year <- as.integer(str_extract(fiscal_year, "(\\d{4})"))
  
  year
  
}

# Generate the matching fiscal year ("2021-2022") from a given date
get_fiscal_year_from_date <- function(date) {
  
  start_year <- get_short_fiscal_year_from_date(date)
  return(convert_start_year_to_fiscal_year(start_year))
  
}



# Generate the matching reporting period ("2021-2022-Q3") from a given date
get_reporting_period_from_date <- function(date) {
  return(str_c(get_fiscal_year_from_date(date), "-", get_quarter_from_date(date)))
}



# CSV output helper functions =============================

# Rounding helpers for CSV exports, adapted from
# https://github.com/GoC-Spending/contracts-data/blob/main/lib/exports.R#L28
option_round_totals_digits <- 2
option_round_percentages_digits <- 4

# Rounds any column ending in "total" to 2 decimal places
# Thanks to
# https://dplyr.tidyverse.org/reference/across.html
exports_round_totals <- function(input_df) {
  input_df <- input_df %>%
    mutate(
      across(ends_with(c("total", "value", "dollars", "years")), ~ format(round(.x, digits = !!option_round_totals_digits), nsmall = !!option_round_totals_digits, trim = TRUE))
    )
  
  return(input_df)
}

# Rounds any column ending in "percentage" to 4 decimal places
exports_round_percentages <- function(input_df) {
  input_df <- input_df %>%
    mutate(
      across(ends_with("percentage"), ~ round(.x, digits = !!option_round_percentages_digits))
    )
  
  return(input_df)
}

# Removes columns that are likely to contain sensitive information from all output CSVs. (Still be careful and test, though!)
output_safety_check <- function(df) {
  
  df <- df %>%
    select(! any_of(c(
      "full_name", 
      "notes"
      )))
  
  df
  
}

# Writes a CSV file to the "out/" directory, and returns the data if necessary for future piped functions.
write_out_csv <- function(df, filename, na = "") {
  
  df %>%
    output_safety_check() %>%
    exports_round_totals() %>%
    exports_round_percentages() %>%
    write_csv(str_c("out/", filename, ".csv"), na = na)
  
  df
  
}



# Other helper functions =============================

# Assumes tenure_data data shape (`cds_start_date` and `cds_end_date`, one row per person-tour)
filter_to_people_present_on_date <- function(df, date_to_check) {
  df %>%
    filter(cds_end_date_or_present > date_to_check, cds_start_date <= date_to_check)
}
