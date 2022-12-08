library(tidyverse)
library(janitor)
library(lubridate)
library(googlesheets4)
library(glue)
library(fs)

source("lib/import-trello-data.R")
source("lib/helpers.R")

# Read Google Sheets URLs from a .env file instead of hardcoding them in.
# See .env.example for details
if(file_exists(".env")) {
  readRenviron(".env")
  
  sheet_url_tenure_data <- Sys.getenv("sheet_url_tenure_data")
  sheet_url_level_data <- Sys.getenv("sheet_url_level_data")
  sheet_url_level_name_harmonization_data <- Sys.getenv("sheet_url_level_name_harmonization_data")
  
} else {
  stop("No .env environment file exists")
}

ggsave_default_options <- function(filename, custom_height = 6.5) {
  ggsave(filename, dpi = "print", width = 6.5, height = custom_height, units = "in")
  
}

# Load data from external sources ===============

# Switch back to TRUE to export a slightly cleaner version of the Trello CSV data:
# import_trello_csv_data("source/trello-cds-on-boarding-2022-09-20.csv", FALSE)

# Get source data from Drive
tenure_data <- read_sheet(sheet_url_tenure_data) %>%
  mutate(across(ends_with("date"), date)) # fix for date interpretation with read_sheet

tenure_data <- tenure_data %>%
  clean_names()

# Get group and level data from Drive
# IMPORTANT NOTE: this group and level data isn't organized by "tour" the way the tenure data is.
# When analyzed, it should be done on a per-individual basis instead of per-role basis.
level_data <- read_sheet(sheet_url_level_data, col_types = "c") %>%
  clean_names() %>%
  mutate(
    earliest_date = case_when(
      earliest_date == "1900-01-00" ~ NA_character_,
      TRUE ~ earliest_date
      ),
    latest_date = case_when(
      latest_date == "1900-01-00" ~ NA_character_,
      TRUE ~ latest_date
    )
  ) %>%
  mutate(across(ends_with("date"), date))

# Remove spurious entries (like "Global Forecast")
# Only leaves entries with a comma in the "name" field
level_data <- level_data %>%
  filter(str_detect(name, ",")) %>% 
  filter(!is.na(earliest_date))

# Split comma-separated names into first/last names
level_data <- level_data %>%
  separate(
    name, c("last_name", "first_name"), sep = ","
  ) %>%
  mutate(across(ends_with("name"), str_to_title))

# Rejoin into space-separated full names
level_data <- level_data %>%
  relocate("first_name", "last_name", everything()) %>% 
  unite(col = "full_name", ends_with("name"), sep = " ")

# For easier merging, make an uppercase version of folks' names
level_data <- level_data %>%
  mutate(
    full_name_uc = str_to_upper(full_name)
  ) %>% 
  select(!full_name)

tenure_data <- tenure_data %>%
  mutate(
    full_name_uc = str_to_upper(full_name)
  )

# Identify mismatched names in both sets
# level_names <- setdiff(level_data$full_name_uc, tenure_data$full_name_uc) %>%
#   as_tibble() %>%
#   arrange(value)
# 
# tenure_names <- setdiff(tenure_data$full_name_uc, level_data$full_name_uc) %>%
#   as_tibble() %>%
#   arrange(value)

# Bring in name harmonization data
level_name_harmonization_data <- read_sheet(sheet_url_level_name_harmonization_data, col_types = "c")

# Join in the tenure data-matching harmonization names
tenure_data <- tenure_data %>% 
  left_join(level_name_harmonization_data, by = c("full_name_uc" = "tenure_data_full_name"))

# Fill in any names that didn't need the harmonization data
tenure_data <- tenure_data %>% 
  mutate(
    level_data_full_name = case_when(
      !is.na(level_data_full_name) ~ level_data_full_name,
      TRUE ~ full_name_uc
    )
  )

# Join group/level data using the combined list
tenure_data <- tenure_data %>% 
  left_join(level_data, by = c("level_data_full_name" = "full_name_uc")) %>% 
  select(! c(full_name_uc, level_data_full_name)) %>% 
  # Remove the earliest_date and latest_date columns that originate from the group/level data, they're less consistent than the tenure data dates:
  select(! c(earliest_date, latest_date))


# Load (approximate, slightly out of date) salary information for promotion analysis
core_public_service_pay_rates <- read_csv("https://raw.githubusercontent.com/meetingcostcalculator/meeting-cost-calculator-data/master/ca/rates/core.csv")

# Some entries have extra separators, like DA-PRO-2
core_public_service_pay_rates <- core_public_service_pay_rates %>% 
  separate(col = "label", sep = "-", into = c("interim_group", "interim_level", "interim_extra_level"), fill = "right")

# Handle extra separators and bring back together groups like DA-PRO
core_public_service_pay_rates <- core_public_service_pay_rates %>% 
  mutate(
    group = case_when(
      !is.na(interim_extra_level) ~ str_c(interim_group, "-", interim_level),
      TRUE ~ interim_group
    ),
    level = case_when(
      !is.na(interim_extra_level) ~ interim_extra_level,
      TRUE ~ interim_level
    )
  )

# Remove interim columns used above
core_public_service_pay_rates <- core_public_service_pay_rates %>% 
  select(! starts_with("interim")) %>% 
  relocate(group, level, everything())

# Remove leading zeroes
# Note, there are a few outstanding issues with inconsistently-formatted names like "PI-1-CGC"
core_public_service_pay_rates <- core_public_service_pay_rates %>% 
  mutate(level = str_remove(level, "^0+")) %>% 
  mutate(level = as.integer(level))

# Rename "CS" classification group to "IT"
core_public_service_pay_rates <- core_public_service_pay_rates %>% 
  mutate(
    group = case_when(
      group == "CS" ~ "IT",
      TRUE ~ group
    )
  )

# Rename "STUDENT" classification to "SU" for consistency
core_public_service_pay_rates <- core_public_service_pay_rates %>% 
  mutate(
    group = case_when(
      group == "STUDENT" ~ "SU",
      TRUE ~ group
    )
  )

# Create a median pay rate to use in calculations below
core_public_service_pay_rates <- core_public_service_pay_rates %>% 
  group_by(group, level) %>% 
  mutate(
    median = median(c(min, max))
  ) %>% 
  ungroup()

# Initial data cleanup ==========================

# Update the onboarding status for pre-Trello board employees that were onboarded
tenure_data <- tenure_data %>%
  mutate(
    onboarding_status = case_when(
      onboarding_status == "pre_trello_board" ~ "onboarded",
      TRUE ~ onboarding_status
    )
  )

# Remove employees that ultimately did not onboard
# And, at least for now, skip employees that haven't onboarded yet.
# (Functionally, this is the same as filtering to just "onboarded".)
tenure_data <- tenure_data %>%
  filter(onboarding_status != "not_onboarded") %>%
  filter(! onboarding_status %in% c("incoming", "start_date_confirmed"))

# If tour number isn't set, set it to 1
# If employee has departed, but departure_type isn't set, set it to "unknown_departure_type"
tenure_data <- tenure_data %>%
  mutate(
    tour_number = case_when(
      is.na(tour_number) ~ 1,
      TRUE ~ tour_number
    ),
    departure_type = case_when(
      !is.na(cds_end_date) & is.na(departure_type) ~ "unknown_departure_type",
      TRUE ~ departure_type
    )
  )

# Set the default initial staffing program to avoid NA issues
tenure_data <- tenure_data %>%
  mutate(
    initial_staffing_program = case_when(
      is.na(initial_staffing_program) ~ "(default)",
      TRUE ~ initial_staffing_program
    )
  ) 

# For pasting convenience, capitalize the discipline categories
tenure_data <- tenure_data %>%
  mutate(
    discipline_initial = case_when(
      discipline_initial == "admin-operations" ~ "admin / operations",
      TRUE ~ discipline_initial
    ),
    discipline_initial = str_to_title(discipline_initial)
  )

# Clean up student group/level designations
# Note: we're arbitrarily using "3" as the level for students (approx. undergrad rate) to make it easier to calculate promotions later in the analysis.
tenure_data <- tenure_data %>%
  mutate(
    earliest_group_level = case_when(
      str_detect(earliest_group_level, "SU") ~ "SU-3",
      TRUE ~ earliest_group_level
    ),
    latest_group_level = case_when(
      str_detect(latest_group_level, "SU") ~ "SU-3",
      TRUE ~ latest_group_level
    ),
  )

# Split group and level into two columns
# Leaves NA entries for level for student roles (that don't have a level number)
tenure_data <- tenure_data %>%
  separate(
    earliest_group_level, c("earliest_group", "earliest_level"), sep = "-", fill = "right"
  ) %>% 
  separate(
    latest_group_level, c("latest_group", "latest_level"), sep = "-", fill = "right"
  ) %>% 
  mutate(mutate(across(ends_with("level"), as.integer)))

# Convert CS entries to IT for historical consistency
tenure_data <- tenure_data %>%
  mutate(
    earliest_group = case_when(
      earliest_group == "CS" ~ "IT",
      TRUE ~ earliest_group
    ),
    latest_group = case_when(
      latest_group == "CS" ~ "IT",
      TRUE ~ latest_group
    )
  )

# Merge in example salaries to group and level
# First, skip the descriptions and other columns in the core_public_service_pay_rates table
core_public_service_pay_rates_median <- core_public_service_pay_rates %>% 
  select(group, level, median)

# Match on earliest group and level
tenure_data <- tenure_data %>% 
  left_join(core_public_service_pay_rates_median, by = c("earliest_group" = "group", "earliest_level" = "level")) %>% 
  rename(earliest_median_salary = "median")

tenure_data <- tenure_data %>% 
  left_join(core_public_service_pay_rates_median, by = c("latest_group" = "group", "latest_level" = "level")) %>% 
  rename(latest_median_salary = "median")

tenure_data <- tenure_data %>% 
  mutate(
    salary_change_percentage = (latest_median_salary - earliest_median_salary) / earliest_median_salary
  )

# Analysis ================================================

# See e.g.
# https://www.bamboohr.com/blog/key-hr-metrics
# https://www.indeed.com/career-advice/career-development/hr-metrics

add_start_end_fiscal_years <- function(df) {
  
  df %>%
    mutate(
      cds_start_fiscal_year = get_short_fiscal_year_from_date(cds_start_date),
      cds_end_fiscal_year = get_short_fiscal_year_from_date(cds_end_date)
    )
  
}

add_duration_days_years <- function(df, use_today_for_na_end_dates = FALSE) {
  
  if(use_today_for_na_end_dates == TRUE) {
    # Note: depends on add_functional_end_date_of_today_for_duration_calculations having been run on the same DF first to make sure the cds_end_date_or_present column is present.
    df %>%
      mutate(
        duration_days = as.integer(cds_end_date_or_present - cds_start_date + 1),
        duration_years = duration_days / 365
      )
  }
  else {
    # This remains NA for employees without an end date
    # which is helpful.
    df %>%
      mutate(
        duration_days = as.integer(cds_end_date - cds_start_date + 1),
        duration_years = duration_days / 365
      )
  }
  
}

add_functional_end_date_of_today_for_duration_calculations <- function(df) {
  
  df %>%
    mutate(
      cds_end_date_or_present = case_when(
        is.na(cds_end_date) ~ today(),
        TRUE ~ cds_end_date
      )
    )
  
}

tenure_data <- tenure_data %>%
  add_start_end_fiscal_years()

# To calculate years when employees have been present, we need to include an "end date" of today for the fiscal year calculations/complete function below.
tenure_data <- tenure_data %>%
  add_functional_end_date_of_today_for_duration_calculations()

# Include duration days/years for staff that have left
tenure_data <- tenure_data %>%
  add_duration_days_years()

# Note: any extra columns added to tenure_data should be added to the "complete" call below, to avoid breaking subsequent "distinct" calls.
tenure_data_by_fiscal_year <- tenure_data %>%
  pivot_longer(
    c(cds_start_date, cds_end_date_or_present),
    values_to = "date",
    names_to = NULL
  ) %>%
  group_by(full_name, tour_number) %>%
  complete(date = full_seq(date, 1), nesting(
    # full_name, 
    discipline_initial, 
    initial_staffing_program, 
    location_initial, 
    location_most_recent,
    cds_start_fiscal_year,
    cds_end_fiscal_year,
    onboarding_status,
    cds_end_date,
    # tour_number,
    departure_type,
    notes,
    earliest_group,
    earliest_level,
    latest_group,
    latest_level,
    earliest_median_salary,
    latest_median_salary,
    salary_change_percentage,
    duration_days,
    duration_years,
    arrival_source
    )) %>%
  ungroup() %>% 
  relocate(date)

# Add fiscal years for each of the individual dates
tenure_data_by_fiscal_year <- tenure_data_by_fiscal_year %>%
  mutate(
    fiscal_year = get_short_fiscal_year_from_date(date)
  )

# Remove date entries and swap back down to one entry per employee per tour number per fiscal year.
tenure_data_by_fiscal_year <- tenure_data_by_fiscal_year %>%
  select(! date) %>%
  distinct()

# Per-fiscal year location (averaged out between location_initial and location_most_recent, which assumes people moved about halfway through their fiscal year-based terms, if they changed locations.)
tenure_data_by_fiscal_year <- tenure_data_by_fiscal_year %>%
  group_by(full_name, tour_number) %>%
  mutate(
    location_averaged = case_when(
      fiscal_year < round(mean(fiscal_year)) ~ location_initial,
      TRUE ~ location_most_recent
    )
  ) %>%
  ungroup() %>%
  relocate(
    full_name:location_most_recent,
    location_averaged,
    everything()
  )


# Total arrivals onboarded at CDS =========================

total_onboarded <- tenure_data %>%
  select(full_name, tour_number) %>% 
  distinct() %>% 
  count(name = "total_onboarded") %>%
  write_out_csv("arrivals")

total_individuals <- tenure_data %>%
  select(full_name) %>% 
  distinct() %>% 
  count(name = "total_individual_staff") %>%
  write_out_csv("headcount_individual_staff")

tenure_data %>%
  select(full_name, tour_number, arrival_source) %>% 
  distinct() %>% 
  mutate(
    arrival_source = case_when(
      is.na(arrival_source) ~ "unknown_arrival_source",
      str_sub(arrival_source, 1L, 1L) == "?" ~ "unknown_arrival_source",
      TRUE ~ arrival_source
    )
  ) %>%
  group_by(arrival_source) %>%
  count(name = "onboarded_count") %>%
  ungroup() %>%
  mutate(
    total_count = sum(onboarded_count)
  ) %>%
  mutate(
    percentage = onboarded_count / total_count,
  ) %>%
  arrange(desc(onboarded_count)) %>%
  write_out_csv("arrivals_by_source")

# Total departures to date ================================

tenure_data %>%
  select(full_name, cds_end_date, departure_type) %>% 
  filter(! is.na(cds_end_date)) %>%
  filter(departure_type != "to_cds_full_time") %>%
  distinct() %>% 
  count(name = "total_departed") %>%
  write_out_csv("departures")

# Number of starts and departures by fiscal year ==========

# Arrivals by fiscal year
arrivals_by_fiscal_year <- tenure_data_by_fiscal_year %>%
  select(full_name, cds_start_fiscal_year) %>%
  distinct() %>%
  group_by(cds_start_fiscal_year) %>%
  count(name = "arrivals") %>%
  write_out_csv("arrivals_by_fiscal_year")

# Departures by fiscal year
departures_by_fiscal_year <- tenure_data_by_fiscal_year %>%
  select(full_name, cds_end_fiscal_year) %>%
  distinct() %>%
  group_by(cds_end_fiscal_year) %>%
  count(name = "departures") %>%
  filter(! is.na(cds_end_fiscal_year)) %>%
  write_out_csv("departures_by_fiscal_year")

# Current headcount (as of this analysis/source data)
# Take all the departures and keep everyone with an "NA" departure year - we're still here!
# Note: replaced with the simpler approach below!
# tenure_data_by_fiscal_year %>%
#   select(full_name, cds_end_fiscal_year) %>%
#   distinct() %>%
#   group_by(cds_end_fiscal_year) %>%
#   count(name = "departures") %>%
#   filter(is.na(cds_end_fiscal_year)) %>%
#   ungroup() %>%
#   rename(
#     current_headcount = "departures"
#   ) %>%
#   select(current_headcount)

# Alternative/simpler current headcount calculation
headcount_data <- tenure_data %>%
  filter(is.na(cds_end_date)) %>%
  group_by(full_name) %>%
  mutate(
    most_recent_start_date = last(cds_start_date, order_by = cds_start_date)
  ) %>%
  ungroup() %>%
  select(full_name, most_recent_start_date) %>%
  distinct()

headcount_current <- headcount_data %>%
  count(name = "headcount_current") %>%
  write_out_csv("headcount_current")

headcount_joined_within_past_12_months <- headcount_data %>%
  filter(most_recent_start_date >= today() - 365) %>%
  count(name = "headcount_joined_within_past_12_months")

tibble(headcount_joined_within_past_12_months, headcount_current) %>%
  mutate(
    percentage = headcount_joined_within_past_12_months / headcount_current
  ) %>%
  write_out_csv("headcount_joined_within_past_12_months")


# Current headcount by discipline (using the most recent discipline for people with multiple tours)
tenure_data_by_fiscal_year %>%
  group_by(full_name) %>%
  mutate(
    most_recent_discipline_initial = last(discipline_initial, order_by = tour_number)
  ) %>%
  ungroup() %>%
  select(full_name, most_recent_discipline_initial, cds_end_fiscal_year) %>%
  distinct() %>%
  filter(is.na(cds_end_fiscal_year)) %>%
  rename(
    discipline_initial = "most_recent_discipline_initial"
  ) %>%
  group_by(discipline_initial) %>%
  count(name = "count_by_discipline") %>%
  ungroup() %>%
  mutate(
    total_count = sum(count_by_discipline)
  ) %>%
  mutate(
    percentage = count_by_discipline / total_count,
    percentage = round(percentage, digits = 4)
  ) %>%
  arrange(desc(count_by_discipline)) %>%
  write_out_csv("headcount_by_discipline")

# Headcount by fiscal year ================================

# Note: this is "peak" headcount, e.g. the number of distinct individuals who were at CDS *during* the respective fiscal years. (*Not* the total number present at the end of each fiscal year.)

headcount_by_fiscal_year <- tenure_data_by_fiscal_year %>%
  select(full_name, fiscal_year) %>%
  distinct() %>%
  group_by(fiscal_year) %>%
  count(name = "headcount") %>%
  write_out_csv("headcount_by_fiscal_year")

headcount_by_fiscal_year

# Disciplinary distribution by fiscal year ================

tenure_data_by_fiscal_year %>%
  select(full_name, discipline_initial, fiscal_year) %>%
  distinct() %>%
  group_by(fiscal_year, discipline_initial) %>%
  count(name = "discipline_count") %>%
  write_out_csv("discipline_count_by_fiscal_year")

# Arrivals by discipline =====================================

arrivals_by_discipline <- tenure_data %>%
  select(full_name, discipline_initial) %>%
  distinct() %>%
  group_by(discipline_initial) %>%
  count(name = "arrivals_by_discipline_count") %>%
  ungroup() %>%
  mutate(
    total_count = sum(arrivals_by_discipline_count)
  ) %>%
  mutate(
    percentage = arrivals_by_discipline_count / total_count,
    percentage = round(percentage, digits = 4)
  ) %>%
  arrange(desc(arrivals_by_discipline_count)) %>%
  write_out_csv("arrivals_by_discipline")

# New-hire turnover =======================================

# (e.g. how many leave within, e.g., 6 or 12months -- students excluded)
tenure_data_departures <- tenure_data %>%
  filter(! is.na(cds_end_date)) %>%
  filter(initial_staffing_program != "student") %>%
  filter(departure_type != "to_cds_full_time")

tenure_data_departures %>%
  count(name = "total_departed") %>%
  write_out_csv("departures_excluding_students")

tenure_data_departures <- tenure_data_departures %>%
  mutate(
    departed_under_1_year = case_when(
      duration_years < 1 ~ 1,
      TRUE ~ 0
    )
  )

# Note that onboarded numbers don't quite match 
tenure_data_departures %>%
  group_by(departed_under_1_year) %>%
  count(name = "departure_count") %>%
  mutate(
    total_onboarded = !!total_onboarded$total_onboarded
  ) %>%
  mutate(
    departure_percentage = departure_count / total_onboarded,
    departure_percentage = round(departure_percentage, digits = 4)
  ) %>%
  write_out_csv("new_hire_turnover_under_1_year")

tenure_data_departures %>%
  group_by(discipline_initial, departed_under_1_year) %>%
  count(name = "departure_count") %>%
  ungroup() %>%
  group_by(discipline_initial) %>%
  mutate(
    total_departure_count = sum(departure_count)
  ) %>%
  ungroup() %>%
  filter(departed_under_1_year == 1) %>%
  select(! departed_under_1_year) %>%
  rename(
    departed_under_1_year_count = "departure_count"
  ) %>%
  left_join(arrivals_by_discipline, by = "discipline_initial") %>% 
  select(! c(total_count, percentage)) %>%
  mutate(
    departed_under_1_year_percentage = departed_under_1_year_count / arrivals_by_discipline_count,
    departed_under_1_year_percentage = round(departed_under_1_year_percentage, digits = 4)
  ) %>%
  arrange(desc(departed_under_1_year_percentage)) %>%
  write_out_csv("new_hire_turnover_under_1_year_by_discipline")


# Number of tours =========================================

# (for multiple tours, breakdown by initial staffing type)

tenure_data_multiple_tours <- tenure_data %>%
  group_by(full_name) %>%
  mutate(
    highest_tour_number = last(tour_number, order_by = tour_number),
    first_discipline_initial = first(discipline_initial, order_by = tour_number),
    first_initial_staffing_program = first(initial_staffing_program, order_by = tour_number)
  ) %>%
  ungroup() %>%
  select(full_name, highest_tour_number, first_discipline_initial, first_initial_staffing_program) %>%
  distinct() %>%
  rename(
    discipline_initial = "first_discipline_initial",
    initial_staffing_program = "first_initial_staffing_program"
  )

tenure_data_multiple_tours %>%
  filter(highest_tour_number > 1) %>%
  group_by(discipline_initial) %>%
  count(name = "multiple_tours_by_discipline_count", sort = TRUE) %>%
  write_out_csv("multiple_tours_by_discipline")

tenure_data_multiple_tours %>%
  filter(highest_tour_number > 1) %>%
  group_by(initial_staffing_program) %>%
  count(name = "multiple_tours_by_initial_staffing_program_count", sort = TRUE) %>%
  write_out_csv("multiple_tours_by_initial_staffing_program")

# Number of students that did multiple tours vs. number of students overall
# Note that this includes students that are currently at CDS under "highest_tour_number=1", and slightly under-weighs returns as a result
tenure_data_multiple_tours %>%
  filter(initial_staffing_program == "student") %>%
  group_by(highest_tour_number) %>%
  count(name = "students_count") %>%
  write_out_csv("multiple_tours_students_tour_number")

# Location (NCR versus non-NCR) by fiscal year ============

location_by_fiscal_year <- tenure_data_by_fiscal_year %>%
  select(full_name, location_averaged, fiscal_year) %>%
  # Attempt to avoid counting the same person in two locations in the same fiscal year; this doesn't appear to change the results:
  group_by(full_name, fiscal_year) %>%
  mutate(
    location_averaged = last(location_averaged)
  ) %>%
  ungroup() %>%
  distinct() %>%
  group_by(fiscal_year, location_averaged) %>%
  count(name = "location_count")

calculate_in_or_out_of_national_capital_region <- function(df) {
  df %>% 
    mutate(
      in_or_out_of_national_capital_region = case_when(
        location_averaged == "Ottawa-Gatineau" ~ "in_ncr",
        TRUE ~ "outside_ncr"
      )
    )
}

location_in_out_ncr_by_fiscal_year <- location_by_fiscal_year %>%
  calculate_in_or_out_of_national_capital_region() %>%
  group_by(fiscal_year, in_or_out_of_national_capital_region) %>%
  mutate(
    location_in_out_count = sum(location_count)
  ) %>%
  ungroup() %>% 
  select(fiscal_year, in_or_out_of_national_capital_region, location_in_out_count) %>%
  distinct() %>%
  group_by(fiscal_year) %>%
  mutate(
    total_staff_count = sum(location_in_out_count)
  ) %>%
  arrange(fiscal_year, in_or_out_of_national_capital_region) %>%
  mutate(
    location_percentage = location_in_out_count / total_staff_count,
    location_percentage = round(location_percentage, digits = 4)
  ) %>%
  write_out_csv("location_in_out_ncr_by_fiscal_year")

# Number of staff that moved while at CDS =================

# (Group together individuals by name, compare initial and most recent location information)
tenure_data %>%
  group_by(full_name) %>%
  mutate(
    first_location = first(location_initial, order_by = tour_number),
    most_recent_location = last(location_most_recent, order_by = tour_number)
  ) %>%
  select(full_name, first_location, most_recent_location) %>%
  distinct() %>%
  mutate(
    location_changed_at_cds = case_when(
      first_location != most_recent_location ~ 1,
      TRUE ~ 0
    )
  ) %>%
  group_by(location_changed_at_cds) %>%
  count(name = "count") %>%
  write_out_csv("location_changed_while_at_cds")

# Departure type by discipline ============================

# (Note, this dataframe excludes student hires)
tenure_data_departures %>%
  group_by(departure_type) %>%
  count(name = "departure_count") %>%
  ungroup() %>%
  mutate(
    total_count = sum(departure_count)
  ) %>%
  mutate(
    departure_type_percentage = departure_count / total_count,
    departure_type_combined = str_c(
      round(departure_type_percentage * 100, digits = 0),
      "% ",
      "(",
      departure_count,
      ")"
    )
  ) %>%
  arrange(desc(departure_count)) %>%
  write_out_csv("departure_type")

# (Note, this dataframe excludes student hires)
departure_type_by_discipline <- tenure_data_departures %>%
  group_by(discipline_initial, departure_type) %>%
  count(name = "departure_count") %>%
  group_by(discipline_initial) %>%
  mutate(
    total_count = sum(departure_count)
  ) %>%
  mutate(
    departure_type_percentage = departure_count / total_count
  ) %>%
  write_out_csv("departure_type_by_discipline")

departure_type_by_arrival_source <- tenure_data_departures %>%
  group_by(arrival_source, departure_type) %>%
  count(name = "departure_count") %>%
  group_by(arrival_source) %>%
  mutate(
    total_count = sum(departure_count)
  ) %>%
  mutate(
    departure_type_percentage = departure_count / total_count
  ) %>%
  write_out_csv("departure_type_by_arrival_source")

# Pivot summary of departure_type_by_discipline percentages for use in the research document
departure_type_by_discipline %>%
  mutate(
    departure_type_percentage = round(departure_type_percentage, digits = 4),
    departure_type_combined = str_c(
      round(departure_type_percentage * 100, digits = 0),
      "% ",
      "(",
      departure_count,
      ")"
    )
  ) %>%
  select(! c(departure_type_percentage, departure_count, total_count)) %>%
  pivot_wider(
    names_from = "departure_type",
    values_from = "departure_type_combined",
    values_fill = "0%"
  ) %>%
  relocate(
    discipline_initial,
    to_other_department,
    to_other_government,
    to_private_sector,
    starts_with("to_"),
    everything()
  ) %>%
  write_out_csv("summary_departure_type_by_discipline_percentages")

# Similar pivot summary for departure_type_by_arrival_source
departure_type_by_arrival_source %>%
  mutate(
    departure_type_percentage = round(departure_type_percentage, digits = 4),
    departure_type_combined = str_c(
      round(departure_type_percentage * 100, digits = 0),
      "% ",
      "(",
      departure_count,
      ")"
    )
  ) %>%
  select(! c(departure_type_percentage, departure_count, total_count)) %>%
  pivot_wider(
    names_from = "departure_type",
    values_from = "departure_type_combined",
    values_fill = "0%"
  ) %>%
  relocate(
    arrival_source,
    to_academia,
    to_non_profit,
    to_other_department,
    to_other_government,
    to_private_sector,
    unknown_departure_type,
    everything()
  ) %>%
  filter(arrival_source != "from_other_staffing_program") %>%
  write_out_csv("summary_departure_type_by_arrival_source_percentages")


# Departure types for staff that arrived from outside government
# Note that this currently excludes "to CDS full time" employees
# and also people that are still here!
tenure_data_departures %>%
  mutate(
    arrival_from_outside_government = case_when(
      arrival_source %in% c("from_private_sector", "from_non_profit", "from_academia ") ~ 1,
      arrival_source %in% c("from_other_department", "from_other_government", "from_other_staffing_program") ~ 0,
      TRUE ~ NA_real_
    )
  ) %>% 
  filter(arrival_from_outside_government == 1) %>%
  group_by(departure_type) %>%
  count(name = "departure_count") %>%
  ungroup() %>%
  mutate(
    total_count = sum(departure_count)
  ) %>%
  mutate(
    departure_type_percentage = departure_count / total_count
  ) %>%
  arrange(desc(departure_count)) %>%
  write_out_csv("departure_type_joined_from_outside_government")

# Multi-variable summaries ================================

location_percent_out_of_ncr_by_fiscal_year <- location_in_out_ncr_by_fiscal_year %>%
  filter(in_or_out_of_national_capital_region == "outside_ncr") %>%
  select(fiscal_year, location_percentage)

# Combined summary
arrivals_by_fiscal_year %>%
  left_join(departures_by_fiscal_year, by = c(cds_start_fiscal_year = "cds_end_fiscal_year")) %>%
  rename(
    fiscal_year = "cds_start_fiscal_year"
  ) %>%
  group_by(fiscal_year) %>%
  mutate(
    new_hires_percentage = (arrivals - departures) / arrivals,
    replacement_hires_percentage = 1 - new_hires_percentage
  ) %>%
  ungroup() %>%
  left_join(headcount_by_fiscal_year, by = "fiscal_year") %>%
  rename(
    headcount_arrivals = "headcount"
  ) %>%
  mutate(
    headcount_adjusted = headcount_arrivals - departures
  ) %>%
  left_join(location_percent_out_of_ncr_by_fiscal_year, by = "fiscal_year") %>%
  rename(
    location_outside_ncr_percentage = "location_percentage"
  ) %>%
  mutate(
    headcount_lagged_average = (lag(headcount_adjusted) + headcount_adjusted) / 2,
    turnover_rate_percentage = departures / headcount_lagged_average
  ) %>%
  write_out_csv("summary_arrivals_departures_headcount_by_fiscal_year")

# Average tenure duration at CDS ==========================

# There's a lot of extra handling here to account for "to_cds_full_time" departure types,
# that are considered a departure in other cases but merged back into a "single tour" for the purpose of calculating overall tenure durations.
tenure_data_duration <- tenure_data %>%
  mutate(
    tour_is_switch_to_full_time = NA_real_
  ) %>%
  mutate(
    tour_is_switch_to_full_time = case_when(
      departure_type == "to_cds_full_time" ~ 1,
      TRUE ~ tour_is_switch_to_full_time
    )
  ) %>%
  arrange(full_name, tour_number) %>%
  mutate(
    tour_is_switch_to_full_time = case_when(
      lag(tour_is_switch_to_full_time) == 1 ~ 1,
      TRUE ~ tour_is_switch_to_full_time
    )
  ) %>%
  group_by(full_name) %>%
  mutate(
    tour_number = case_when(
      tour_is_switch_to_full_time == 1 ~ first(tour_number),
      TRUE ~ tour_number
    )
  ) %>%
  ungroup() %>%
  group_by(full_name, tour_number) %>%
  mutate(
    cds_start_date = first(cds_start_date),
    cds_end_date = last(cds_end_date),
    most_recent_discipline_initial = last(discipline_initial),
    initial_arrival_source = first(arrival_source)
  ) %>%
  ungroup() %>%
  # Note that we're intentionally not including duration_days and duration_years here, so that we can re-generate it based on the combined tour start/end dates
  select(
    full_name,
    tour_number,
    cds_start_date,
    cds_end_date, 
    most_recent_discipline_initial,
    initial_arrival_source
  ) %>%
  distinct() %>%
  add_functional_end_date_of_today_for_duration_calculations() %>%
  add_duration_days_years(TRUE) 

# Tenure at CDS ================================

# For these calculations, we'll use "tenure" to refer to all CDSers (including recent hires) and "duration" to refer to CDSers' average stay minus folks hired within the past 12 months.

# Exclude people that have left to calculate current staff tenure:
tenure_data_duration %>%
  filter(is.na(cds_end_date)) %>%
  summarize(
    average_tenure_years = mean(duration_years)
  ) %>%
  write_out_csv("tenure_average")

# Exclude people that have left, and bucket current people here:
tenure_data_duration %>%
  filter(is.na(cds_end_date)) %>%
  mutate(
    bins_duration_years = cut(
      duration_years, 
      c(0:3, Inf),
      labels = c("Under 1 year", "1-2 years", "2-3 years", "3+ years")
      )
  ) %>% 
  group_by(bins_duration_years) %>%
  count(name = "headcount") %>%
  write_out_csv("tenure_headcount_bins")

# Plots for the tenure duration ===========================
tenure_data_duration %>%
  filter(is.na(cds_end_date)) %>%
  ggplot(aes(x = duration_years)) +
  geom_histogram(binwidth = 1/2) +
  scale_x_continuous(breaks = seq(1:7)) +
  labs(
    x = "Years at CDS",
    y = "Number of employees",
    title = "Employees by years at CDS (employees currently at CDS)",
    subtitle = glue("As of {format(today(), format = '%B %d, %Y')} ({today()})"),
    caption = "plots/p004_tenure_duration_histogram_current.png"
  )

ggsave_default_options("plots/p004_tenure_duration_histogram_current.png", 4)


tenure_data_duration %>%
  ggplot(aes(x = duration_years)) +
  geom_histogram(binwidth = 1/2) +
  scale_x_continuous(breaks = seq(1:7)) +
  ylim(c(0, 50)) +
  labs(
    x = "Years at CDS",
    y = "Number of employees",
    title = "Employees by years at CDS (all employees in CDS history)",
    subtitle = glue("As of {format(today(), format = '%B %d, %Y')} ({today()})"),
    caption = "plots/p004_tenure_duration_histogram_all.png"
  )

ggsave_default_options("plots/p005_tenure_duration_histogram_all.png", 4)
  

# Duration at CDS ===============================

# To avoid weighing the average down with recent arrivals (and short durations-until-today), exclude hires from within the past 1 year from this calculation:
tenure_data_duration_minus_recent_hires <- tenure_data_duration %>%
  filter(cds_start_date < today() - 365)

tenure_data_duration_minus_recent_hires %>%
  summarize(
    average_duration_years = mean(duration_years)
  ) %>%
  write_out_csv("duration_average")

tenure_data_duration_minus_recent_hires %>%
  group_by(most_recent_discipline_initial) %>%
  summarize(
    average_duration_years = mean(duration_years),
    arrivals_count = n()
  ) %>%
  arrange(desc(average_duration_years)) %>%
  write_out_csv("duration_average_by_discipline")

tenure_data_duration_minus_recent_hires %>%
  summarize(duration_years = quantile(duration_years, seq(0, 1, 1/4)), q = seq(0, 1, 1/4)) %>%
  write_out_csv("duration_quantiles")

tenure_data_duration_minus_recent_hires %>%
  filter(!is.na(initial_arrival_source)) %>% 
  group_by(initial_arrival_source) %>%
  summarize(
    average_duration_years = mean(duration_years),
    arrivals_count = n()
  ) %>%
  arrange(desc(average_duration_years)) %>%
  write_out_csv("duration_average_by_arrival_source")


# Group and level comparisons =============================

group_level_data <- tenure_data %>% 
  group_by(full_name) %>%
  mutate(
    most_recent_discipline_initial = last(discipline_initial, order_by = tour_number),
    most_recent_location_most_recent = last(location_most_recent, order_by = tour_number),
    most_recent_arrival_source = last(arrival_source, order_by = tour_number),
  ) %>%
  ungroup() %>%
  select(
    full_name, 
    most_recent_discipline_initial, 
    most_recent_location_most_recent,
    most_recent_arrival_source,
    earliest_group,
    earliest_level,
    latest_group,
    latest_level,
    earliest_median_salary,
    latest_median_salary,
    salary_change_percentage
    ) %>%
  distinct() %>%
  rename(
    discipline_initial = "most_recent_discipline_initial",
    location_most_recent = "most_recent_location_most_recent",
    arrival_source = "most_recent_arrival_source"
  )

# Exclude current students and any staff (e.g. early CDSers) that aren't included in the group/level data from calculations
group_level_data_with_latest_level <- group_level_data %>% 
  filter(!is.na(latest_level))

group_level_data_with_latest_level <- group_level_data_with_latest_level %>% 
  mutate(
    has_promotion = case_when(
      latest_median_salary > earliest_median_salary ~ TRUE,
      TRUE ~ FALSE
    ),
    has_classification_change = case_when(
      earliest_group != latest_group ~ TRUE,
      TRUE ~ FALSE
    ),
    has_level_increase = case_when(
      latest_level > earliest_level ~ TRUE,
      TRUE ~ FALSE
    ),
    has_classification_change_or_level_increase = case_when(
      has_classification_change | has_level_increase == TRUE ~ TRUE,
      TRUE ~ FALSE
    )
  )

# Original calculation using the combination of a classification change or a level increase (assuming that these generally indicate a salary increase / promotion)
calculate_classification_change_or_level_increase <- function(df) {
  
  df %>% 
    summarize(
      has_classification_change_or_level_increase = sum(has_classification_change_or_level_increase),
      total_count = n()
    ) %>% 
    mutate(
      percentage = has_classification_change_or_level_increase / total_count
    ) %>% 
    arrange(desc(percentage))
    
}

# New calculation using estimated (earliest and latest) salary totals
calculate_promotion_totals <- function(df) {
  
  df %>% 
    summarize(
      has_promotion_count = sum(has_promotion),
      total_count = n()
    ) %>% 
    mutate(
      percentage = has_promotion_count / total_count
    ) %>% 
    arrange(desc(percentage), desc(has_promotion_count))
  
}

group_level_data_with_latest_level %>% 
  calculate_promotion_totals %>% 
  write_out_csv("classification_promotion")

group_level_data_with_latest_level %>% 
  filter(earliest_group != "SU") %>% 
  calculate_promotion_totals %>% 
  write_out_csv("classification_promotion_excluding_students")

# Note: excludes groups with 0 changes from the CSV export
# This uses the *latest* group for analysis, not individuals' starting group
group_level_data_with_latest_level %>% 
  group_by(latest_group) %>% 
  calculate_promotion_totals %>% 
  filter(percentage != 0) %>% 
  write_out_csv("classification_promotion_by_latest_group")

# Same as above but excluding folks who started as students from calculations
group_level_data_with_latest_level %>% 
  filter(earliest_group != "SU") %>% 
  group_by(latest_group) %>% 
  calculate_promotion_totals %>% 
  filter(percentage != 0) %>% 
  write_out_csv("classification_promotion_excluding_students_by_latest_group")

# Uses initial discipline categories to group, since we don't have latest/downstream discipline categories
group_level_data_with_latest_level %>% 
  group_by(discipline_initial) %>% 
  calculate_promotion_totals %>% 
  write_out_csv("classification_promotion_by_discipline_initial")

# Same as above but excluding folks who started as students from calculations
group_level_data_with_latest_level %>% 
  filter(earliest_group != "SU") %>% 
  group_by(discipline_initial) %>% 
  calculate_promotion_totals %>% 
  write_out_csv("classification_promotion_excluding_students_by_discipline_initial")

# Comparing promotion rates in and outside the NCR
group_level_data_with_latest_level %>% 
  # Renaming for compatibility with the calculate_in_or_out_of_national_capital_region function
  rename(location_averaged = "location_most_recent") %>% 
  calculate_in_or_out_of_national_capital_region() %>%
  group_by(in_or_out_of_national_capital_region) %>% 
  calculate_promotion_totals %>% 
  write_out_csv("classification_promotion_by_in_or_out_of_ncr")

group_level_data_with_latest_level %>% 
  filter(earliest_group != "SU") %>% 
  # Renaming for compatibility with the calculate_in_or_out_of_national_capital_region function
  rename(location_averaged = "location_most_recent") %>% 
  calculate_in_or_out_of_national_capital_region() %>%
  group_by(in_or_out_of_national_capital_region) %>% 
  calculate_promotion_totals %>% 
  write_out_csv("classification_promotion_excluding_students_by_in_or_out_of_ncr")


# Plots for the arrival sources ===========================
# Thanks to @lchski! 

ggsave_default_options <- function(filename, custom_height = 6.5) {
  ggsave(filename, dpi = "print", width = 6.5, height = custom_height, units = "in")
  
}

replace_arrival_source_labels <- function(df) {
  
  arrival_source_labels = tribble(
    ~arrival_source, ~arrival_source_label,
    "from_academia", "Academia",
    "from_non_profit", "Non-profit sector",
    "from_other_department", "Other GC departments",
    "from_other_government", "Other levels of government",
    "from_other_staffing_program", "Other staffing program",
    "from_private_sector", "Private sector",
    "other", "Unknown",
  )
  
  df <- df %>%
    left_join(arrival_source_labels, by = "arrival_source") %>%
    select(! arrival_source) %>%
    rename(
      arrival_source = "arrival_source_label"
    )
  
  df
  
}



set_na_arrival_source_to_unknown <- function(df) {
  
  df %>%
    mutate(
      arrival_source = case_when(
        is.na(arrival_source) ~ "other",
        TRUE ~ arrival_source
      )
    )
  
}

# By proportion
tenure_data %>%
  set_na_arrival_source_to_unknown() %>%
  replace_arrival_source_labels() %>%
  group_by(cds_start_fiscal_year) %>%
  count(arrival_source) %>%
  mutate(prop = round(n / sum(n), 2)) %>%
  ggplot(aes(x = cds_start_fiscal_year, y = prop, color = arrival_source, fill = arrival_source)) +
  geom_point() +
  geom_line() +
  ylim(c(0, 0.85)) +
  labs(
    x = "Fiscal year",
    y = "Proportion",
    color = "Arrival source",
    fill = "Arrival source",
    title = "Employee source (%) by fiscal year",
    subtitle = glue("As of {format(today(), format = '%B %d, %Y')} ({today()})"),
    caption = "plots/p001_arrival_source_proportion.png"
  )

ggsave_default_options("plots/p001_arrival_source_proportion.png", 4)

# By count
tenure_data %>%
  set_na_arrival_source_to_unknown() %>%
  replace_arrival_source_labels() %>%
  group_by(cds_start_fiscal_year) %>%
  count(arrival_source) %>%
  mutate(prop = round(n / sum(n), 2)) %>%
  ggplot(aes(x = cds_start_fiscal_year, y = n, color = arrival_source, fill = arrival_source)) +
  geom_point() +
  geom_line() +
  labs(
    x = "Fiscal year",
    y = "Staff arrival counts",
    color = "Arrival source",
    fill = "Arrival source",
    title = "Employee source (#) by fiscal year",
    subtitle = glue("As of {format(today(), format = '%B %d, %Y')} ({today()})"),
    caption = "plots/p002_arrival_source_count.png"
  )

ggsave_default_options("plots/p002_arrival_source_count.png", 4)

# In vs. outside the NRC by year
replace_location_labels <- function(df) {
  
  source_labels = tribble(
    ~in_or_out_of_national_capital_region, ~in_or_out_of_national_capital_region_label,
    "in_ncr", "National Capital Region",
    "outside_ncr", "Outside the NCR",
  )
  
  df <- df %>%
    left_join(source_labels, by = "in_or_out_of_national_capital_region") %>%
    select(! in_or_out_of_national_capital_region) %>%
    rename(
      in_or_out_of_national_capital_region = "in_or_out_of_national_capital_region_label"
    )
  
  df
  
}


location_in_out_ncr_by_fiscal_year %>%
  replace_location_labels() %>%
  ggplot(aes(x = fiscal_year, y = location_in_out_count, color = in_or_out_of_national_capital_region, fill = in_or_out_of_national_capital_region)) +
  geom_point() +
  geom_line() +
  labs(
    x = "Fiscal year",
    y = "Staff headcount",
    color = "Location",
    fill = "Location",
    title = "Employee location (new hires) by fiscal year, outside / within NCR",
    subtitle = glue("As of {format(today(), format = '%B %d, %Y')} ({today()})"),
    caption = "plots/p003_location_in_out_ncr_count.png"
  )

ggsave_default_options("plots/p003_location_in_out_ncr_count.png", 4)


headcount_by_month <- tibble(
  month = seq.Date(ymd("2016-09-01"), today(), by = "1 month")
) %>%
  mutate(
    people = map(
      month, ~ tenure_data %>% filter_to_people_present_on_date(.x)
    ),
    headcount = map_int(people, nrow)
  ) %>%
  mutate(
    change_prev_month = headcount - lag(headcount, default = 0),
    change_prev_year = headcount - lag(headcount, n = 12, default = 0),
    pct_change_month = round(change_prev_month / lag(headcount, default = 0) * 100, 2),
    pct_change_year = round(change_prev_year / lag(headcount, n = 12, default = 0) * 100, 2)
  )

headcount_by_month %>%
  ggplot(aes(x = month, y = headcount)) +
  geom_line() +
  geom_smooth() +
  scale_x_date(
    breaks = seq.Date(ymd("2016-04-01"), today(), by = "12 months"),
    date_labels = "%b '%y"
  ) +
  labs(
    x = "Month",
    y = "Headcount on first of month",
    title = "Headcount on first of month",
    subtitle = glue("As of {format(today(), format = '%B %d, %Y')} ({today()})"),
    caption = "plots/p006_headcount_monthly.png"
  )

ggsave_default_options("plots/p006_headcount_monthly.png", 4)


headcount_by_month %>%
  ggplot(aes(x = month, y = change_prev_month)) +
  geom_point() +
  geom_smooth() +
  scale_x_date(
    breaks = seq.Date(ymd("2016-04-01"), today(), by = "12 months"),
    date_labels = "%b '%y"
  ) +
  ylim(c(-5, 10)) +
  labs(
    x = "Month",
    y = "Change since previous month",
    title = "Change in first-of-month headcount",
    subtitle = glue("As of {format(today(), format = '%B %d, %Y')} ({today()})"),
    caption = "plots/p007_headcount_change_monthly.png"
  )

ggsave_default_options("plots/p007_headcount_change_monthly.png", 4)  



policy_headcount_by_month <- tibble(
  month = seq.Date(ymd("2016-09-01"), today(), by = "1 month")
) %>%
  mutate(
    people = map(
      month, ~ tenure_data %>% filter(discipline_initial == "Policy") %>% filter_to_people_present_on_date(.x)
    ),
    headcount = map_int(people, nrow)
  ) %>%
  mutate(
    change_prev_month = headcount - lag(headcount, default = 0),
    change_prev_year = headcount - lag(headcount, n = 12, default = 0),
    pct_change_month = round(change_prev_month / lag(headcount, default = 0) * 100, 2),
    pct_change_year = round(change_prev_year / lag(headcount, n = 12, default = 0) * 100, 2)
  )
