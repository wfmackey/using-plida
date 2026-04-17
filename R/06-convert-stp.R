# Convert STP data to parquet: 2 year files + the jobs file.
# STP weekly files are ~48 GB each — same streaming approach as MBS.

source("R/00-paths.R")
source("R/helpers.R")

suppressPackageStartupMessages({
  library(arrow); library(fs); library(glue)
})

log_file <- file.path(paths$bench_dir, "convert-log.parquet")

convert_one <- function(csv_path, out_dir, label, basename_prefix) {
  dir_create(out_dir)
  message(glue("  converting {path_file(csv_path)} ({round(path_size_gb(csv_path), 1)} GB)"))
  gc(full = TRUE, verbose = FALSE)
  rss0 <- rss_gb(); t0 <- Sys.time()
  ds <- open_csv_dataset(csv_path)
  write_dataset(ds, path = out_dir, format = "parquet",
                basename_template = glue("{basename_prefix}-{{i}}.parquet"),
                max_rows_per_file = 5e6,
                existing_data_behavior = "overwrite")
  t1 <- Sys.time(); rss1 <- rss_gb()
  tibble(
    task = "convert", approach = "arrow-stream", label = label,
    file = path_file(csv_path), csv_gb = path_size_gb(csv_path),
    parquet_gb = sum(as.numeric(dir_info(out_dir, glob = glue("*{basename_prefix}*.parquet"))$size)) / 1024^3,
    seconds = as.numeric(difftime(t1, t0, units = "secs")),
    rss_peak_gb = max(rss0, rss1, na.rm = TRUE),
    skipped = FALSE, error = NA_character_
  )
}

jobs <- list(
  list(csv = file.path(paths$csv_root, "ato-stp/madipge-ato-d-stp-fy2021.csv"),
       dir = file.path(paths$parquet_root, "stp"), label = "stp-fy2021", prefix = "stp-fy2021"),
  list(csv = file.path(paths$csv_root, "ato-stp/madipge-ato-d-stp-fy2122.csv"),
       dir = file.path(paths$parquet_root, "stp"), label = "stp-fy2122", prefix = "stp-fy2122"),
  list(csv = file.path(paths$csv_root, "ato-stp/madipge-ato-d-stp-jobs-2020-current.csv"),
       dir = file.path(paths$parquet_root, "stp-jobs"), label = "stp-jobs", prefix = "stp-jobs"),
  list(csv = file.path(paths$csv_root, "ato-stp/ato-spine.csv"),
       dir = file.path(paths$parquet_root, "ato-spine-stp"), label = "ato-spine-stp", prefix = "ato-spine-stp")
)

for (j in jobs) {
  res <- convert_one(j$csv, j$dir, j$label, j$prefix)
  print(res)
  log_result(res, log_file)
  gc(full = TRUE, verbose = FALSE)
}
message("done.")
