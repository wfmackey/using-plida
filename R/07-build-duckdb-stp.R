# Add STP tables to plida_tables.duckdb.
#
# Sorting all 5 years (~1B rows) in one shot OOMs the disk spill (needs
# ~200 GB temp). Instead we ingest year-by-year with per-year sorting.
# This gives data that is sorted within each year — good enough for
# DuckDB's window function engine which sorts within partitions anyway.

source("R/00-paths.R")
source("R/helpers.R")

suppressPackageStartupMessages({
  library(duckdb); library(DBI); library(dplyr); library(dbplyr)
  library(fs); library(glue)
})

db_path  <- file.path(paths$duckdb_dir, "plida_tables.duckdb")
log_file <- file.path(paths$bench_dir, "duckdb-build-log.parquet")

con <- dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)
on.exit(try(dbDisconnect(con, shutdown = TRUE), silent = TRUE), add = TRUE)

dbExecute(con, "SET threads TO 8")
dbExecute(con, "SET memory_limit = '20GB'")

time_sql <- function(label, sql) {
  message(glue("  -> {label}"))
  gc(full = TRUE, verbose = FALSE)
  rss0 <- rss_gb(); t0 <- Sys.time()
  dbExecute(con, sql)
  t1 <- Sys.time(); rss1 <- rss_gb()
  row <- tibble(
    task = "build", approach = "duckdb-table", label = label,
    seconds = as.numeric(difftime(t1, t0, units = "secs")),
    rss_peak_gb = max(rss0, rss1, na.rm = TRUE),
    db_size_gb = path_size_gb(db_path),
    n_rows = NA_real_
  )
  print(row)
  log_result(row, log_file)
}

# ---- STP weekly: year-by-year sorted ingest ----------------------------

dbExecute(con, "DROP TABLE IF EXISTS stp")

stp_years <- c("fy2021", "fy2122", "fy2223", "fy2324", "fy2425")
stp_globs <- file.path(paths$parquet_root, "stp", paste0("stp-", stp_years, "*.parquet"))

# Create the table from the first year (sorted)
time_sql(
  "stp year 1 (create + sort)",
  glue("CREATE TABLE stp AS
        SELECT * FROM read_parquet('{stp_globs[1]}')
        ORDER BY SYNTHETIC_AEUID, WEEK_ENDING")
)

# Append remaining years, each sorted
for (i in 2:length(stp_globs)) {
  time_sql(
    glue("stp year {i} (insert + sort)"),
    glue("INSERT INTO stp
          SELECT * FROM read_parquet('{stp_globs[i]}')
          ORDER BY SYNTHETIC_AEUID, WEEK_ENDING")
  )
}

n_stp <- tbl(con, "stp") |> summarise(n = n()) |> pull(n)
message(glue("stp total rows: {scales::comma(n_stp)}"))

# ---- STP jobs ----------------------------------------------------------

jobs_glob <- file.path(paths$parquet_root, "stp-jobs/*.parquet")
dbExecute(con, "DROP TABLE IF EXISTS stp_jobs")
time_sql("stp_jobs", glue("CREATE TABLE stp_jobs AS SELECT * FROM read_parquet('{jobs_glob}')"))

# ---- ATO spine for STP ------------------------------------------------

ato_spine_stp_glob <- file.path(paths$parquet_root, "ato-spine-stp/*.parquet")
existing <- dbListTables(con)
if (!"ato_spine_stp" %in% existing && length(Sys.glob(ato_spine_stp_glob)) > 0) {
  time_sql("ato_spine_stp", glue("CREATE TABLE ato_spine_stp AS SELECT * FROM read_parquet('{ato_spine_stp_glob}')"))
}

message(glue("db size: {round(path_size_gb(db_path), 2)} GB"))
message("done.")
