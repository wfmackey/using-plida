# Convert remaining STP year files (FY2223, FY2324, FY2425).
# FY2021 and FY2122 were converted in 06-convert-stp.R.

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
  list(csv = file.path(paths$csv_root, "ato-stp/madipge-ato-d-stp-fy2223.csv"),
       dir = file.path(paths$parquet_root, "stp"), label = "stp-fy2223", prefix = "stp-fy2223"),
  list(csv = file.path(paths$csv_root, "ato-stp/madipge-ato-d-stp-fy2324.csv"),
       dir = file.path(paths$parquet_root, "stp"), label = "stp-fy2324", prefix = "stp-fy2324"),
  list(csv = file.path(paths$csv_root, "ato-stp/madipge-ato-d-stp-fy2425.csv"),
       dir = file.path(paths$parquet_root, "stp"), label = "stp-fy2425", prefix = "stp-fy2425")
)

for (j in jobs) {
  res <- convert_one(j$csv, j$dir, j$label, j$prefix)
  print(res)
  log_result(res, log_file)
  gc(full = TRUE, verbose = FALSE)
}
message("done.")
