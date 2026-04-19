# Convert a subset of the fplida CSVs to parquet.
#
# We do NOT convert the full 751 GB – only the files needed for the main
# benchmarks and walkthrough:
#
#   ato-pit_itr/*          the four ITR logical tables, split by schema
#   dhda-mbs/2015 claims   the 45 GB stress test fact table
#
# Approach: DuckDB's CSV reader plus `COPY ... TO parquet`.
# On the local six-file benchmark panel (3 large MBS + 3 large STP files),
# this was about 2x faster than Arrow's CSV -> parquet path while producing
# slightly smaller output. We keep zstd compression, medium row groups, and
# medium-sized files so downstream scans and joins still behave well.

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

target_from_itr_name <- function(filename) {
  case_when(
    grepl("context", filename, ignore.case = TRUE)     ~ "itr_context",
    grepl("ded-exp-off", filename, ignore.case = TRUE) ~ "itr_ded",
    grepl("inc-loss", filename, ignore.case = TRUE)    ~ "itr_inc",
    grepl("whld-debt", filename, ignore.case = TRUE)   ~ "itr_whld",
    TRUE ~ NA_character_
  )
}

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

  message(glue("  converting {path_file(csv_path)} ({round(path_size_gb(csv_path), 1)} GB)"))

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

jobs <- list()

# ITR: route each CSV directly to its logical parquet subdir.
itr_csvs <- dir_ls(file.path(paths$csv_root, "ato-pit_itr"), glob = "*.csv")
itr_csvs <- itr_csvs[!grepl("ato-spine", itr_csvs)]

for (f in itr_csvs) {
  nm <- path_file(f)
  target <- target_from_itr_name(nm)
  if (is.na(target)) next

  year_tag <- sub(".*fy([0-9]{4}-[0-9]{2}|[0-9]{4}).*", "\\1", nm)
  prefix <- glue("{target}-{year_tag}")

  jobs[[length(jobs) + 1]] <- list(
    csv = f,
    dir = file.path(paths$parquet_root, target),
    label = target,
    prefix = prefix
  )
}

# MBS 2015 – the main stress-test file used throughout the site.
jobs[[length(jobs) + 1]] <- list(
  csv = file.path(paths$csv_root, "dhda-mbs/madipge-mbs-d-claims-2015.csv"),
  dir = file.path(paths$parquet_root, "dhda-mbs"),
  label = "dhda-mbs-2015",
  prefix = "mbs-2015"
)

message(glue("running {length(jobs)} conversion jobs"))

for (j in jobs) {
  res <- convert_one(j$csv, j$dir, j$label, j$prefix)
  log_result(res, log_file)
  print(res)
  gc(full = TRUE, verbose = FALSE)
}

message("done.")
