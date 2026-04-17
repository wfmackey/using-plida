# Benchmark: LAG() vs self-join for employer-switch detection.
#
# Finding: LAG() OOMs on both the full table (3.5B rows, disk spill
# exceeded 166 GB) AND on 1 year (700M rows, 24 GB memory limit
# exceeded). The window function's full-table sort is simply infeasible
# at this scale on a 32 GB machine.
#
# The self-join avoids the sort entirely. We test it on the full table.

source("R/00-paths.R")
source("R/helpers.R")

suppressPackageStartupMessages({
  library(duckdb); library(DBI); library(dplyr); library(dbplyr)
  library(fs); library(glue)
})

db_path  <- file.path(paths$duckdb_dir, "plida_tables.duckdb")
log_file <- file.path(paths$bench_dir, "window-benchmark-log.parquet")
if (file_exists(log_file)) file_delete(log_file)

con <- dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)
on.exit(try(dbDisconnect(con, shutdown = TRUE), silent = TRUE), add = TRUE)

dbExecute(con, "SET threads TO 8")
dbExecute(con, "SET memory_limit = '20GB'")
tmp_dir <- file.path(paths$offline_root, "duckdb_tmp")
dir_create(tmp_dir)
dbExecute(con, glue("SET temp_directory = '{tmp_dir}'"))

n_all <- tbl(con, "stp") |> summarise(n = n()) |> pull(n)
message(glue("Total STP rows: {scales::comma(n_all)}"))

log_one <- function(approach, seconds, rss1, n_result, error_msg = NA_character_) {
  row <- tibble(
    task = "window_bench", approach = approach,
    seconds = seconds, rss_after_gb = rss1,
    rss_delta_gb = NA_real_, n_result = as.integer(n_result),
    error = error_msg, timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
  print(row)
  log_result(row, log_file)
}

# ---- (a) LAG on 1 year — expect OOM, record the failure ---------------

message("\n=== LAG() on 1 year (expect OOM) ===")
dbExecute(con, "DROP TABLE IF EXISTS switches_lag_1yr")
gc(full = TRUE, verbose = FALSE)
t0 <- Sys.time()
lag_err <- tryCatch({
  dbExecute(con, "
    CREATE TABLE switches_lag_1yr AS
    WITH subset AS (
      SELECT SYNTHETIC_AEUID, WEEK_ENDING, ABN_HASH_TRUNC,
             PMT_SUMRY_TOTL_GRS_PMT_AMT AS gross_pay
      FROM stp
      WHERE WEEK_ENDING >= '2021-07-01' AND WEEK_ENDING < '2022-07-01'
        AND ABN_HASH_TRUNC IS NOT NULL
    ),
    lagged AS (
      SELECT *, LAG(ABN_HASH_TRUNC) OVER (
        PARTITION BY SYNTHETIC_AEUID ORDER BY WEEK_ENDING
      ) AS prev_employer
      FROM subset
    )
    SELECT SYNTHETIC_AEUID, WEEK_ENDING,
           ABN_HASH_TRUNC AS new_employer, prev_employer, gross_pay
    FROM lagged
    WHERE prev_employer IS NOT NULL AND ABN_HASH_TRUNC != prev_employer
  ")
  NULL
}, error = function(e) conditionMessage(e))
t1 <- Sys.time(); rss1 <- rss_gb()
lag_time <- as.numeric(difftime(t1, t0, units = "secs"))

if (is.null(lag_err)) {
  lag_n <- tbl(con, "switches_lag_1yr") |> summarise(n = n()) |> pull(n)
  message(glue("  LAG 1yr: {round(lag_time, 1)}s, {scales::comma(lag_n)} switches"))
  log_one("LAG (1 year)", lag_time, rss1, lag_n)
} else {
  message(glue("  LAG 1yr FAILED ({round(lag_time, 1)}s): {substr(lag_err, 1, 80)}"))
  log_one("LAG (1 year)", lag_time, rss1, NA_integer_, lag_err)
}

# ---- (b) Self-join on 1 year ------------------------------------------

message("\n=== Self-join on 1 year ===")
dbExecute(con, "DROP TABLE IF EXISTS switches_join_1yr")
gc(full = TRUE, verbose = FALSE)
t0 <- Sys.time()
join_1yr_err <- tryCatch({
  dbExecute(con, "
    CREATE TABLE switches_join_1yr AS
    SELECT
      t1.SYNTHETIC_AEUID, t1.WEEK_ENDING,
      t1.ABN_HASH_TRUNC AS new_employer,
      t2.ABN_HASH_TRUNC AS prev_employer,
      t1.PMT_SUMRY_TOTL_GRS_PMT_AMT AS gross_pay
    FROM stp t1
    INNER JOIN stp t2
      ON t1.SYNTHETIC_AEUID = t2.SYNTHETIC_AEUID
      AND t1.WEEK_ENDING = t2.WEEK_ENDING + INTERVAL '7 days'
    WHERE t1.WEEK_ENDING >= '2021-07-01' AND t1.WEEK_ENDING < '2022-07-01'
      AND t1.ABN_HASH_TRUNC IS NOT NULL
      AND t2.ABN_HASH_TRUNC IS NOT NULL
      AND t1.ABN_HASH_TRUNC != t2.ABN_HASH_TRUNC
  ")
  NULL
}, error = function(e) conditionMessage(e))
t1 <- Sys.time(); rss1 <- rss_gb()
join_1yr_time <- as.numeric(difftime(t1, t0, units = "secs"))

if (is.null(join_1yr_err)) {
  join_1yr_n <- tbl(con, "switches_join_1yr") |> summarise(n = n()) |> pull(n)
  message(glue("  JOIN 1yr: {round(join_1yr_time, 1)}s, {scales::comma(join_1yr_n)} switches"))
  log_one("Self-join (1 year)", join_1yr_time, rss1, join_1yr_n)
} else {
  message(glue("  JOIN 1yr FAILED: {substr(join_1yr_err, 1, 80)}"))
  log_one("Self-join (1 year)", join_1yr_time, rss1, NA_integer_, join_1yr_err)
}

# ---- (c) Self-join on FULL table --------------------------------------

message("\n=== Self-join on FULL table (3.5B rows) ===")
dbExecute(con, "DROP TABLE IF EXISTS switches_join_full")
gc(full = TRUE, verbose = FALSE)
t0 <- Sys.time()
join_full_err <- tryCatch({
  dbExecute(con, "
    CREATE TABLE switches_join_full AS
    SELECT
      t1.SYNTHETIC_AEUID, t1.WEEK_ENDING,
      t1.ABN_HASH_TRUNC AS new_employer,
      t2.ABN_HASH_TRUNC AS prev_employer,
      t1.PMT_SUMRY_TOTL_GRS_PMT_AMT AS gross_pay
    FROM stp t1
    INNER JOIN stp t2
      ON t1.SYNTHETIC_AEUID = t2.SYNTHETIC_AEUID
      AND t1.WEEK_ENDING = t2.WEEK_ENDING + INTERVAL '7 days'
    WHERE t1.ABN_HASH_TRUNC IS NOT NULL
      AND t2.ABN_HASH_TRUNC IS NOT NULL
      AND t1.ABN_HASH_TRUNC != t2.ABN_HASH_TRUNC
  ")
  NULL
}, error = function(e) conditionMessage(e))
t1 <- Sys.time(); rss1 <- rss_gb()
join_full_time <- as.numeric(difftime(t1, t0, units = "secs"))

if (is.null(join_full_err)) {
  join_full_n <- tbl(con, "switches_join_full") |> summarise(n = n()) |> pull(n)
  message(glue("  JOIN full: {round(join_full_time, 1)}s, {scales::comma(join_full_n)} switches"))
  log_one("Self-join (full 3.5B)", join_full_time, rss1, join_full_n)
} else {
  message(glue("  JOIN full FAILED: {substr(join_full_err, 1, 80)}"))
  log_one("Self-join (full 3.5B)", join_full_time, rss1, NA_integer_, join_full_err)
}

# ---- Record LAG-full as infeasible -----------------------------------

log_one("LAG (full 3.5B)", NA_real_, NA_real_, NA_integer_,
        "infeasible: disk spill exceeded 166 GB on prior attempt")

# ---- Summary ----------------------------------------------------------

message("\n=== SUMMARY ===")
message("done.")
