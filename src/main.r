library(jsonlite)
library(duckdb)
library(duckplyr)
library(dbplyr)

# Fetch data uit URL en save naar data.json
# Gebruik bestaande data.json als fallback
url <- "https://maritimecybersecurity.nl/public/allItems"
tryCatch({
  mcad_data <- fromJSON(url, flatten = TRUE)
  write(toJSON(mcad_data, auto_unbox = TRUE), "data/data.json")
}, error = function(e) {
  warning("Kon data niet ophalen van URL, fallback naar data.json: ", e$message)
  mcad_data <- fromJSON("data/data.json", flatten = TRUE)
})

con <- dbConnect(duckdb(), dbdir = "data/mcad.duckdb")

# Maak een incidenten table van de titel, methode, jaar en positie
incidents_table <- mcad_data %>%
  select(referenceNumber, title, method, year, position.lat, position.lng) %>%
  mutate(
    referenceNumber = as.integer(referenceNumber),
    title = as.character(title),
    method = as.character(method),
    year = as.integer(year),
    position.lat = as.double(position.lat),
    position.lng = as.double(position.lng)
  )

# Maak een slachtoffer table van de identiteit, land, type en gebied
victims_table <- mcad_data %>%
  select(referenceNumber, identity, viccountry, type, area) %>%
  mutate(
    referenceNumber = as.integer(referenceNumber),
    identity = as.character(identity),
    viccountry = as.character(viccountry),
    type = as.character(type),
    area = as.character(area)
  )

# Check wat er al in de database zit
existing_refs <- tryCatch({
  dbReadTable(con, "incidents")$referenceNumber
}, error = function(e) {
  integer(0)
})

# Filter voor nieuwe data
new_incidents <- incidents_table[!incidents_table$referenceNumber %in% existing_refs, ]
new_victims <- victims_table[!victims_table$referenceNumber %in% existing_refs, ]

# Print nieuwe rows
cat("Nieuwe incidents:", nrow(new_incidents), "rows\n")
cat("Nieuwe victims:", nrow(new_victims), "rows\n")

# Print aantal rows
cat("Incidents:", nrow(incidents_table), "rows\n")
cat("Victims:", nrow(victims_table), "rows\n")

# Save alleen nieuwe data naar DuckDB
# Rollback bij error
invisible(if (nrow(new_incidents) > 0) {
  dbBegin(con)
  tryCatch({
    dbWriteTable(con, "incidents", new_incidents, append = TRUE)
    dbWriteTable(con, "victims", new_victims, append = TRUE)

    # Join tables op basis van referenceNumber
    joined_table <- con %>%
      tbl("incidents") %>%
      left_join(con %>% tbl("victims"), by = "referenceNumber")

    # Filter de joined table op rows na 2020
    filtered_table <- joined_table %>%
      filter(year > 2020)

    # Aggregeer de joined table voor incidenten per jaar
    aggregated_table <- joined_table %>%
      group_by(year) %>%
      summarise(count = n())

    # Drop verouderde tables
    invisible(dbExecute(con, "DROP TABLE IF EXISTS joined"))
    invisible(dbExecute(con, "DROP TABLE IF EXISTS filtered"))
    invisible(dbExecute(con, "DROP TABLE IF EXISTS aggregated"))

    # Bereken en save nieuwe tables vanuit DuckDB
    joined_table <- compute(joined_table, name = "joined", temporary = FALSE)
    filtered_table <- compute(filtered_table, name = "filtered", temporary = FALSE)
    aggregated_table <- compute(aggregated_table, name = "aggregated", temporary = FALSE)
    dbCommit(con)
  }, error = function(e) {
    dbRollback(con)
    stop(e)
  })
})

# Print aantal rows
cat("Joined:", con %>% tbl("joined") %>% tally() %>% pull(), "rows\n")
cat("Filtered:", con %>% tbl("filtered") %>% tally() %>% pull(), "rows\n")
cat("Aggregated:", con %>% tbl("aggregated") %>% tally() %>% pull(), "rows\n")

dbDisconnect(con)


