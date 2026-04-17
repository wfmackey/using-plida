# Centralised paths for the project.
# No "big" data lives in the project folder — all intermediate outputs go to
# a user-configured working directory alongside the fplida source data.
#
# Users set two paths:
#   fplida_path  — where fplida wrote its output (CSV or parquet)
#   work_path    — where this project stores intermediate parquet + DuckDB files
#
# These are configured in 00-setup.qmd and expected to be set before sourcing
# this file. If not set, we fall back to environment variables or error.

suppressPackageStartupMessages({
  library(fs)
})

# Source user paths from R/_common.R if not already set.
# Users edit R/_common.R once; everything else reads from it.
if (!exists("fplida_path") || !exists("work_path")) {
  common_file <- file.path(here::here(), "R", "_common.R")
  if (file.exists(common_file)) {
    source(common_file, local = TRUE)
  }
}

if (!exists("fplida_path") || fplida_path == "") {
  fplida_path <- Sys.getenv("FPLIDA_PATH", unset = "")
  if (fplida_path == "") stop("fplida_path not set. Edit R/_common.R or see 00-setup.qmd.")
}

if (!exists("work_path") || work_path == "") {
  work_path <- Sys.getenv("USING_PLIDA_WORK_PATH", unset = "")
  if (work_path == "") stop("work_path not set. Edit R/_common.R or see 00-setup.qmd.")
}

paths <- list(
  csv_root     = fplida_path,
  offline_root = work_path,
  parquet_root = file.path(work_path, "parquet"),
  duckdb_dir   = file.path(work_path, "duckdb"),
  duckdb_file  = file.path(work_path, "duckdb", "plida_tables.duckdb"),
  bench_dir    = file.path(here::here(), "benchmarks")
)

dir_create(paths$parquet_root)
dir_create(paths$duckdb_dir)
dir_create(paths$bench_dir)

path_size_gb <- function(p) {
  if (is_dir(p)) {
    as.numeric(dir_info(p, recurse = TRUE)$size |> sum()) / 1024^3
  } else {
    as.numeric(file_info(p)$size) / 1024^3
  }
}
