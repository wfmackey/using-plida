# Compare Arrow vs DuckDB for CSV -> Parquet conversion on large files.
#
# The project currently uses Arrow streaming for this step. This script tests
# whether DuckDB's CSV reader plus `COPY ... TO parquet` is actually faster on
# the same machine and with broadly similar Parquet layout settings.
#
# We benchmark a mixed panel of large files so the conclusion is not driven by
# one quirky schema:
#   - 3 large MBS yearly claim files
#   - 3 large STP yearly payroll files
#
# Each run writes to a temporary benchmark directory under work_path, records
# wall time and output size, then deletes the output to avoid consuming
# hundreds of GB of disk.

source("R/00-paths.R")
source("R/helpers.R")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(duckdb)
  library(DBI)
  library(fs)
  library(glue)
  library(tibble)
})

log_file <- file.path(paths$bench_dir, "csv-backend-log.parquet")
bench_root <- file.path(paths$offline_root, "csv-backend-bench")
dir_create(bench_root, recurse = TRUE)

large_files <- tribble(
  ~label,        ~csv_path,
  "mbs-2020",    file.path(paths$csv_root, "dhda-mbs/madipge-mbs-d-claims-2020.csv"),
  "mbs-2019",    file.path(paths$csv_root, "dhda-mbs/madipge-mbs-d-claims-2019.csv"),
  "mbs-2018",    file.path(paths$csv_root, "dhda-mbs/madipge-mbs-d-claims-2018.csv"),
  "stp-fy2425",  file.path(paths$csv_root, "ato-stp/madipge-ato-d-stp-fy2425.csv"),
  "stp-fy2324",  file.path(paths$csv_root, "ato-stp/madipge-ato-d-stp-fy2324.csv"),
  "stp-fy2223",  file.path(paths$csv_root, "ato-stp/madipge-ato-d-stp-fy2223.csv")
) |>
  mutate(csv_gb = vapply(csv_path, path_size_gb, numeric(1)))

arrow_out_dir <- function(label) file.path(bench_root, "arrow", label)
duck_out_dir  <- function(label) file.path(bench_root, "duckdb", label)

dir_size_gb <- function(dir) {
  if (!dir_exists(dir)) return(NA_real_)
  files <- dir_ls(dir, recurse = TRUE, glob = "*.parquet", type = "file")
  if (length(files) == 0) return(NA_real_)
  sum(as.numeric(file_info(files)$size)) / 1024^3
}

dir_n_files <- function(dir) {
  if (!dir_exists(dir)) return(NA_integer_)
  length(dir_ls(dir, recurse = TRUE, glob = "*.parquet", type = "file"))
}

record_row <- function(engine, label, csv_path, seconds, rss_peak_gb, out_dir,
                       error = NA_character_) {
  tibble(
    task = "csv_to_parquet",
    approach = engine,
    label = label,
    file = path_file(csv_path),
    csv_gb = path_size_gb(csv_path),
    parquet_gb = dir_size_gb(out_dir),
    n_files = dir_n_files(out_dir),
    seconds = seconds,
    rss_peak_gb = rss_peak_gb,
    error = error,
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
}

run_arrow <- function(label, csv_path) {
  out_dir <- arrow_out_dir(label)
  if (dir_exists(out_dir)) dir_delete(out_dir)
  dir_create(out_dir, recurse = TRUE)

  gc(full = TRUE, verbose = FALSE)
  rss0 <- rss_gb()
  t0 <- Sys.time()
  err <- tryCatch({
    ds <- open_csv_dataset(csv_path)
    write_dataset(
      ds,
      path = out_dir,
      format = "parquet",
      basename_template = glue("{label}-{{i}}.parquet"),
      max_rows_per_file = 5e6,
      min_rows_per_group = 250000L,
      max_rows_per_group = 250000L,
      compression = "zstd",
      existing_data_behavior = "overwrite"
    )
    NA_character_
  }, error = function(e) conditionMessage(e))
  secs <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  rss1 <- rss_gb()

  row <- record_row(
    engine = "arrow-stream",
    label = label,
    csv_path = csv_path,
    seconds = secs,
    rss_peak_gb = max(rss0, rss1, na.rm = TRUE),
    out_dir = out_dir,
    error = err
  )
  log_result(row, log_file)
  print(row)

  dir_delete(out_dir)
  invisible(row)
}

run_duckdb <- function(label, csv_path) {
  out_dir <- duck_out_dir(label)
  if (dir_exists(out_dir)) dir_delete(out_dir)
  dir_create(out_dir, recurse = TRUE)

  con <- dbConnect(duckdb::duckdb())
  on.exit(try(dbDisconnect(con, shutdown = TRUE), silent = TRUE), add = TRUE)

  dbExecute(con, "SET threads TO 8")
  dbExecute(con, "SET memory_limit = '20GB'")
  dbExecute(con, "SET preserve_insertion_order = false")

  sql <- paste0(
    "COPY (SELECT * FROM read_csv_auto(",
    dbQuoteString(con, csv_path),
    ")) TO ",
    dbQuoteString(con, out_dir),
    " (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE 250000, ROW_GROUPS_PER_FILE 20)"
  )

  gc(full = TRUE, verbose = FALSE)
  rss0 <- rss_gb()
  t0 <- Sys.time()
  err <- tryCatch({
    dbExecute(con, sql)
    NA_character_
  }, error = function(e) conditionMessage(e))
  secs <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  rss1 <- rss_gb()

  row <- record_row(
    engine = "duckdb-copy",
    label = label,
    csv_path = csv_path,
    seconds = secs,
    rss_peak_gb = max(rss0, rss1, na.rm = TRUE),
    out_dir = out_dir,
    error = err
  )
  log_result(row, log_file)
  print(row)

  dbDisconnect(con, shutdown = TRUE)
  on.exit(NULL, add = FALSE)
  dir_delete(out_dir)
  invisible(row)
}

print(large_files)

for (i in seq_len(nrow(large_files))) {
  row <- large_files[i, ]
  message(glue("==> {row$label} ({round(row$csv_gb, 1)} GB)"))
  run_arrow(row$label, row$csv_path)
  gc(full = TRUE, verbose = FALSE)
  run_duckdb(row$label, row$csv_path)
  gc(full = TRUE, verbose = FALSE)
}

message("done.")
