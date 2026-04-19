# Supplementary conversions: the demographic table + id-linker spines.
#
# These are smaller than the MBS / STP fact tables, but we keep the same
# DuckDB CSV -> parquet path as the main converter so the workflow stays
# consistent across the project.

source("R/00-paths.R")
source("R/helpers.R")

suppressPackageStartupMessages({
  library(duckdb)
  library(DBI)
  library(dplyr)
  library(fs)
  library(glue)
})

log_file <- file.path(paths$bench_dir, "convert-log.parquet")

convert_one <- function(csv_path, out_dir, label, basename_prefix) {
  dir_create(out_dir, recurse = TRUE)
  marker <- path(out_dir, glue("{basename_prefix}.done"))

  if (file_exists(marker)) {
    return(tibble(
      task = "convert", approach = "duckdb-copy", label = label,
      file = path_file(csv_path),
      csv_gb = path_size_gb(csv_path),
      parquet_gb = tryCatch(sum(as.numeric(dir_info(out_dir,
                                                    glob = glue("{basename_prefix}-*.parquet"))$size)) / 1024^3,
                            error = function(e) NA_real_),
      seconds = NA_real_, rss_peak_gb = NA_real_,
      skipped = TRUE, error = NA_character_,
      timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    ))
  }

  message(glue("  converting {path_file(csv_path)} ({round(path_size_gb(csv_path), 2)} GB)"))

  tmp_dir <- file.path(out_dir, glue("{basename_prefix}-tmp"))
  if (dir_exists(tmp_dir)) dir_delete(tmp_dir)
  dir_create(tmp_dir, recurse = TRUE)

  con <- dbConnect(duckdb::duckdb())
  on.exit(try(dbDisconnect(con, shutdown = TRUE), silent = TRUE), add = TRUE)
  dbExecute(con, "SET threads TO 8")
  dbExecute(con, "SET memory_limit = '20GB'")
  dbExecute(con, "SET preserve_insertion_order = false")

  sql <- paste0(
    "COPY (SELECT * FROM read_csv_auto(",
    dbQuoteString(con, csv_path),
    ")) TO ",
    dbQuoteString(con, tmp_dir),
    " (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE 250000, ROW_GROUPS_PER_FILE 20)"
  )

  gc(full = TRUE, verbose = FALSE)
  rss0 <- rss_gb()
  t0 <- Sys.time()
  err <- tryCatch({
    dbExecute(con, sql)

    tmp_files <- dir_ls(tmp_dir, glob = "*.parquet", type = "file")
    out_files <- file.path(out_dir, glue("{basename_prefix}-{seq_along(tmp_files) - 1}.parquet"))
    file_move(tmp_files, out_files)
    dir_delete(tmp_dir)
    file_create(marker)
    NA_character_
  }, error = function(e) conditionMessage(e))
  t1 <- Sys.time()
  rss1 <- rss_gb()

  row <- tibble(
    task = "convert", approach = "duckdb-copy", label = label,
    file = path_file(csv_path),
    csv_gb = path_size_gb(csv_path),
    parquet_gb = tryCatch(sum(as.numeric(dir_info(out_dir,
                                                   glob = glue("{basename_prefix}-*.parquet"))$size)) / 1024^3,
                          error = function(e) NA_real_),
    seconds = as.numeric(difftime(t1, t0, units = "secs")),
    rss_peak_gb = max(rss0, rss1, na.rm = TRUE),
    skipped = FALSE,
    error = err,
    timestamp = format(t1, "%Y-%m-%d %H:%M:%S")
  )

  dbDisconnect(con, shutdown = TRUE)
  on.exit(NULL, add = FALSE)
  if (dir_exists(tmp_dir)) dir_delete(tmp_dir)

  row
}

jobs <- list(
  list(csv = file.path(paths$csv_root, "abs-core/plidage-core-demog-cb-c21-2006-latest.csv"),
       dir = file.path(paths$parquet_root, "demo"), label = "demo", prefix = "demo"),
  list(csv = file.path(paths$csv_root, "abs-core/abs-spine.csv"),
       dir = file.path(paths$parquet_root, "abs-spine"), label = "abs-spine", prefix = "abs-spine"),
  list(csv = file.path(paths$csv_root, "ato-pit_itr/ato-spine.csv"),
       dir = file.path(paths$parquet_root, "ato-spine"), label = "ato-spine", prefix = "ato-spine"),
  list(csv = file.path(paths$csv_root, "dhda-mbs/dhda-spine.csv"),
       dir = file.path(paths$parquet_root, "dhda-spine"), label = "dhda-spine", prefix = "dhda-spine")
)

for (j in jobs) {
  res <- convert_one(j$csv, j$dir, j$label, j$prefix)
  print(res)
  log_result(res, log_file)
  gc(full = TRUE, verbose = FALSE)
}

message("done.")
