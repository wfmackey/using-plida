# Build DuckDB databases over the parquet output from step 01/01b.
#
# We build two flavours so we can compare them in the benchmarks:
#
# (A) VIEWS over parquet   -- zero-copy. DuckDB reads parquet on each query.
#                             Build is essentially free. Queries pay the
#                             parquet decode cost every time but DuckDB will
#                             push filters down and skip row groups.
#
# (B) TABLES materialised  -- CREATE TABLE ... AS SELECT * FROM read_parquet.
#                             One-off ingest cost; afterwards queries hit
#                             DuckDB's native storage (with its own zonemaps
#                             and compression).
#
# Convention: we use raw SQL (dbExecute) for DDL — DROP/CREATE/ATTACH — because
# that is the clean way to wire DuckDB up. Analysis queries later use dbplyr.

source("R/00-paths.R")
source("R/helpers.R")

suppressPackageStartupMessages({
  library(duckdb); library(DBI); library(dplyr); library(dbplyr)
  library(fs); library(glue)
})

views_db  <- file.path(paths$duckdb_dir, "plida_views.duckdb")
tables_db <- file.path(paths$duckdb_dir, "plida_tables.duckdb")

log_file <- file.path(paths$bench_dir, "duckdb-build-log.parquet")

# Logical table name -> glob over the parquet files from step 01/01b.
parquet_sources <- list(
  demo       = file.path(paths$parquet_root, "demo/*.parquet"),
  abs_spine  = file.path(paths$parquet_root, "abs-spine/*.parquet"),
  ato_spine  = file.path(paths$parquet_root, "ato-spine/*.parquet"),
  dhda_spine = file.path(paths$parquet_root, "dhda-spine/*.parquet"),
  itr        = file.path(paths$parquet_root, "itr_context/*.parquet"),
  itr_ded    = file.path(paths$parquet_root, "itr_ded/*.parquet"),
  itr_inc    = file.path(paths$parquet_root, "itr_inc/*.parquet"),
  itr_whld   = file.path(paths$parquet_root, "itr_whld/*.parquet"),
  mbs        = file.path(paths$parquet_root, "dhda-mbs/*.parquet")
)

have_any <- function(pattern) length(Sys.glob(pattern)) > 0
parquet_sources <- parquet_sources[vapply(parquet_sources, have_any, logical(1))]
message(glue("sources available: {paste(names(parquet_sources), collapse=', ')}"))

# ---- (A) VIEWS --------------------------------------------------------

message("-- building VIEWS db")
if (file_exists(views_db)) file_delete(views_db)
con <- dbConnect(duckdb::duckdb(), dbdir = views_db, read_only = FALSE)
dbExecute(con, "SET threads TO 8")

rss0 <- rss_gb(); t0 <- Sys.time()
for (nm in names(parquet_sources)) {
  dbExecute(con, glue(
    "CREATE OR REPLACE VIEW {nm} AS SELECT * FROM read_parquet('{parquet_sources[[nm]]}')"
  ))
}
t1 <- Sys.time(); rss1 <- rss_gb()

# sanity: list the registered views via dbplyr
tables_info <- tbl(con, sql("SELECT table_name, table_type FROM information_schema.tables")) |>
  collect()
print(tables_info)

log_result(tibble(
  task = "build", approach = "duckdb-views", label = "all",
  seconds = as.numeric(difftime(t1, t0, units = "secs")),
  rss_peak_gb = max(rss0, rss1, na.rm = TRUE),
  db_size_gb = path_size_gb(views_db),
  n_rows = NA_integer_,
  timestamp = format(t1, "%Y-%m-%d %H:%M:%S")
), log_file)

dbDisconnect(con, shutdown = TRUE)

# ---- (B) TABLES -------------------------------------------------------

message("-- building TABLES db")
if (file_exists(tables_db)) file_delete(tables_db)
con <- dbConnect(duckdb::duckdb(), dbdir = tables_db, read_only = FALSE)
dbExecute(con, "SET threads TO 8")
dbExecute(con, "SET memory_limit = '20GB'")

for (nm in names(parquet_sources)) {
  message(glue("  CREATE TABLE {nm}"))
  gc(full = TRUE, verbose = FALSE)
  rss0 <- rss_gb(); t0 <- Sys.time()
  dbExecute(con, glue(
    "CREATE OR REPLACE TABLE {nm} AS SELECT * FROM read_parquet('{parquet_sources[[nm]]}')"
  ))
  t1 <- Sys.time(); rss1 <- rss_gb()
  # Use dbplyr to count rows (pushed down to DuckDB)
  n <- tbl(con, nm) |> summarise(n = n()) |> pull(n)
  row <- tibble(
    task = "build", approach = "duckdb-table", label = nm,
    seconds = as.numeric(difftime(t1, t0, units = "secs")),
    rss_peak_gb = max(rss0, rss1, na.rm = TRUE),
    db_size_gb = path_size_gb(tables_db),
    n_rows = n,
    timestamp = format(t1, "%Y-%m-%d %H:%M:%S")
  )
  print(row)
  log_result(row, log_file)
}
dbDisconnect(con, shutdown = TRUE)
message("done.")
