library(jsonlite)
library(duckdb)
library(duckplyr)

# Fetch data uit URL en save naar data.json
# Gebruik bestaande data.json als fallback
url <- "https://maritimecybersecurity.nl/public/allItems"
tryCatch({
  mcad_data <- fromJSON(url, flatten = TRUE)
  write(toJSON(mcad_data, auto_unbox = TRUE), "data/data.json")
}, error = function(e) {
  mcad_data <- fromJSON("data/data.json", flatten = TRUE)
})

# Maak een incidenten table van de titel, methode, jaar en positie
incidents_table <- mcad_data %>%
  select(referenceNumber, title, method, year, position.lat, position.lng)

# Maak een slachtoffer table van de identiteit, land, type en gebied
victims_table <- mcad_data %>%
  select(referenceNumber, identity, viccountry, type, area)

# Check wat er al in de database zit
con <- dbConnect(duckdb(), dbdir = "data/mcad.duckdb")
existing_refs <- tryCatch({
  dbReadTable(con, "incidents")$referenceNumber
}, error = function(e) {
  character(0)
})
dbDisconnect(con)

# Filter voor nieuwe data
new_incidents <- incidents_table[!incidents_table$referenceNumber %in% existing_refs, ]
new_victims <- victims_table[!victims_table$referenceNumber %in% existing_refs, ]

# Print nieuwe rows
cat("Nieuwe incidents:", nrow(new_incidents), "rows\n")
cat("Nieuwe victims:", nrow(new_victims), "rows\n")

# Save alleen nieuwe data naar DuckDB
if (nrow(new_incidents) > 0) {
  con <- dbConnect(duckdb(), dbdir = "data/mcad.duckdb")
  dbWriteTable(con, "incidents", new_incidents, append = TRUE)
  dbWriteTable(con, "victims", new_victims, append = TRUE)
  dbDisconnect(con)
}

# Lees tables uit DuckDB
con <- dbConnect(duckdb(), dbdir = "data/mcad.duckdb")
incidents_table <- dbReadTable(con, "incidents")
victims_table <- dbReadTable(con, "victims")
dbDisconnect(con)

# Print aantal rows
cat("Incidents:", nrow(incidents_table), "rows\n")
cat("Victims:", nrow(victims_table), "rows\n")

# Join tables op basis van referenceNumber
joined_table <- left_join(incidents_table, victims_table, by = "referenceNumber")

# Filter de joined table op rows na 2020
filtered_table <- joined_table %>%
  filter(year > 2020)

# Aggregeer de joined table voor incidenten per jaar
aggregated_table <- joined_table %>%
  group_by(year) %>%
  summarise(count = n())

# Sla tables op in DuckDB
con <- dbConnect(duckdb(), dbdir = "data/mcad.duckdb")
dbWriteTable(con, "joined", joined_table, overwrite = TRUE)
dbWriteTable(con, "filtered", filtered_table, overwrite = TRUE)
dbWriteTable(con, "aggregated", aggregated_table, overwrite = TRUE)
dbDisconnect(con)

# Print aantal rows
cat("Joined:", nrow(joined_table), "rows\n")
cat("Filtered:", nrow(filtered_table), "rows\n")
cat("Aggregated:", nrow(aggregated_table), "rows\n")


