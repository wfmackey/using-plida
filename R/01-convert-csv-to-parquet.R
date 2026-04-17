# Convert a subset of the fplida CSVs to parquet.
#
# We do NOT convert the full 751 GB — only the files needed for the
# benchmarks. The subset is:
#
#   base-spine.csv         (11 GB, ~30M rows)     the demographic spine
#   ato-pit_itr/*          (31 GB, ~20 files)     individual tax returns
#   dhda-mbs/2015 claims   (45 GB, bigger than RAM) the stress test
#
# Approach: arrow's open_csv_dataset + write_dataset streams the file in
# batches. Memory stays bounded regardless of input size. We use zstd
# compression and cap row-group size so downstream scans can prune.

source("R/00-paths.R")
source("R/helpers.R")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(fs)
  library(glue)
})

log_file <- file.path(paths$bench_dir, "convert-log.parquet")

convert_one <- function(csv_path, out_dir, label, basename_prefix) {
  dir_create(out_dir)
  marker <- path(out_dir, glue("{basename_prefix}.done"))
  if (file_exists(marker)) {
    return(tibble(
      task = "convert", approach = "arrow-stream", label = label,
      file = path_file(csv_path),
      csv_gb = path_size_gb(csv_path),
      parquet_gb = NA_real_,
      seconds = NA_real_, rss_peak_gb = NA_real_,
      skipped = TRUE, error = NA_character_
    ))
  }
  message(glue("  converting {path_file(csv_path)} ({round(path_size_gb(csv_path),1)} GB)"))
  gc(full = TRUE, verbose = FALSE)
  rss0 <- rss_gb()
  t0 <- Sys.time()
  res <- tryCatch({
    ds <- open_csv_dataset(csv_path)
    write_dataset(
      ds,
      path = out_dir,
      format = "parquet",
      basename_template = glue("{basename_prefix}-{{i}}.parquet"),
      max_rows_per_file = 5e6,
      existing_data_behavior = "overwrite"
    )
    file_create(marker)
    NULL
  }, error = function(e) conditionMessage(e))
  t1 <- Sys.time()
  rss1 <- rss_gb()

  tibble(
    task = "convert", approach = "arrow-stream", label = label,
    file = path_file(csv_path),
    csv_gb = path_size_gb(csv_path),
    parquet_gb = tryCatch(sum(as.numeric(dir_info(out_dir,
                                                   glob = glue("*{basename_prefix}*.parquet"))$size))/1024^3,
                          error = function(e) NA_real_),
    seconds = as.numeric(difftime(t1, t0, units = "secs")),
    rss_peak_gb = max(rss0, rss1, na.rm = TRUE),
    skipped = FALSE,
    error = if (is.null(res)) NA_character_ else res
  )
}

jobs <- list()

# 1) base spine (11 GB)
jobs[[length(jobs) + 1]] <- list(
  csv = file.path(paths$csv_root, "_system/base-spine.csv"),
  dir = file.path(paths$parquet_root, "base-spine"),
  label = "base-spine", prefix = "base-spine"
)

# 2) all ITR year files (small-ish each)
itr_csvs <- dir_ls(file.path(paths$csv_root, "ato-pit_itr"), glob = "*.csv")
itr_year_csvs <- itr_csvs[!grepl("ato-spine", itr_csvs)]
for (f in itr_year_csvs) {
  year_tag <- sub(".*fy([0-9]{4}-[0-9]{2}).*", "\\1", path_file(f))
  jobs[[length(jobs) + 1]] <- list(
    csv = f,
    dir = file.path(paths$parquet_root, "ato-pit_itr"),
    label = "ato-pit_itr",
    prefix = glue("itr-{year_tag}")
  )
}

# 3) MBS 2015 — the 45 GB stress-test file
jobs[[length(jobs) + 1]] <- list(
  csv = file.path(paths$csv_root, "dhda-mbs/madipge-mbs-d-claims-2015.csv"),
  dir = file.path(paths$parquet_root, "dhda-mbs"),
  label = "dhda-mbs-2015", prefix = "mbs-2015"
)

message(glue("running {length(jobs)} conversion jobs"))

for (j in jobs) {
  res <- convert_one(j$csv, j$dir, j$label, j$prefix)
  log_result(res, log_file)
  print(res)
  gc(full = TRUE, verbose = FALSE)
}

message("done.")
