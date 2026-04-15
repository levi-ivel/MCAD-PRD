library(jsonlite)
library(duckdb)
library(duckplyr)
library(dbplyr)

# Fetch data from URL and save to data.json
# Use existing data.json as fallback
url <- "https://maritimecybersecurity.nl/public/allItems"
tryCatch({
  mcad_data <- fromJSON(url, flatten = TRUE)
  write(toJSON(mcad_data, auto_unbox = TRUE), "data/data.json")
}, error = function(e) {
  warning("Could not fetch data from URL, falling back to data.json: ", e$message)
  mcad_data <- fromJSON("data/data.json", flatten = TRUE)
})

con <- dbConnect(duckdb(), dbdir = "data/mcad.duckdb")

# Create incidents table from title, method, year and position
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

# Create victims table from identity, country, type and area
victims_table <- mcad_data %>%
  select(referenceNumber, identity, viccountry, type, area) %>%
  mutate(
    referenceNumber = as.integer(referenceNumber),
    identity = as.character(identity),
    viccountry = as.character(viccountry),
    type = as.character(type),
    area = as.character(area)
  )

# Check what is already in the database
existing_refs <- tryCatch({
  dbReadTable(con, "incidents")$referenceNumber
}, error = function(e) {
  integer(0)
})

# Filter for new data
new_incidents <- incidents_table[!incidents_table$referenceNumber %in% existing_refs, ]
new_victims <- victims_table[!victims_table$referenceNumber %in% existing_refs, ]

cat("New incidents:", nrow(new_incidents), "rows\n")
cat("New victims:", nrow(new_victims), "rows\n")

cat("Incidents:", nrow(incidents_table), "rows\n")
cat("Victims:", nrow(victims_table), "rows\n")

# Save only new data to DuckDB
# Rollback on error
invisible(if (nrow(new_incidents) > 0) {
  dbBegin(con)
  tryCatch({
    dbWriteTable(con, "incidents", new_incidents, append = TRUE)
    dbWriteTable(con, "victims", new_victims, append = TRUE)

    # Join tables on referenceNumber
    joined_table <- con %>%
      tbl("incidents") %>%
      left_join(con %>% tbl("victims"), by = "referenceNumber")

    # Filter joined table for rows after 2020
    filtered_table <- joined_table %>%
      filter(year > 2020)

    # Aggregate joined table for incidents per year
    aggregated_table <- joined_table %>%
      group_by(year) %>%
      summarise(count = n())

    # Drop outdated tables
    invisible(dbExecute(con, "DROP TABLE IF EXISTS joined"))
    invisible(dbExecute(con, "DROP TABLE IF EXISTS filtered"))
    invisible(dbExecute(con, "DROP TABLE IF EXISTS aggregated"))

    # Compute and save new tables from DuckDB
    joined_table <- compute(joined_table, name = "joined", temporary = FALSE)
    filtered_table <- compute(filtered_table, name = "filtered", temporary = FALSE)
    aggregated_table <- compute(aggregated_table, name = "aggregated", temporary = FALSE)

    # Create search index from joined table
    dbExecute(con, "PRAGMA create_fts_index('joined', 'referenceNumber', 'title', 'method', 'viccountry', 'area', overwrite=1)")

    dbCommit(con)
  }, error = function(e) {
    dbRollback(con)
    stop(e)
  })
})

cat("Joined:", con %>% tbl("joined") %>% tally() %>% pull(), "rows\n")
cat("Filtered:", con %>% tbl("filtered") %>% tally() %>% pull(), "rows\n")
cat("Aggregated:", con %>% tbl("aggregated") %>% tally() %>% pull(), "rows\n")

dbDisconnect(con)