# Functions to import Trello onboarding CSV data

import_trello_csv_data <- function(filename, export_csv_output = TRUE) {
  
  trello_data <- read_csv(filename) %>%
    clean_names()
  
  # Remove template cards
  trello_data <- trello_data %>%
    filter(! str_detect(card_name, "Template")) %>%
    filter(! str_detect(card_name, "PLACEHOLDER")) %>%
    select(
      card_name,
      due_date,
      last_activity_date,
      list_name
    )
  
  # From trello data to the intended data structure
  tenure_data <- trello_data %>%
    separate(card_name, into = c("full_name", "role"), sep = "- ", remove = TRUE, extra = "merge", fill = "right") %>%
    mutate(
      full_name = str_trim(full_name),
      role = str_trim(role),
      location = NA_character_,
      cds_start_date = as_date(due_date),
      onboarding_status = case_when(
        list_name == "Pre-First Day" ~ "incoming",
        list_name == "Start Date CONFIRMED" ~ "start_date_confirmed",
        TRUE ~ "onboarded"
      ),
      trello_last_activity_date = as_date(last_activity_date),
      role_start_date = cds_start_date,
      role_end_date = NA_character_,
      cds_end_date = NA_character_,
      departure_type = NA_character_
    ) %>%
    select(! c("due_date", "last_activity_date", "list_name")) %>%
    arrange(trello_last_activity_date)
  
  if(export_csv_output) {
    tenure_data %>%
      write_csv(str_c("source/", today(), "-tenure-data.csv"), na = "")
  }
  
  tenure_data

}
