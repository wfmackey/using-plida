# Focused parquet-layout benchmarks on the ITR context table.
#
# Why this script exists:
#   - the project docs recommend Parquet as the storage layer;
#   - DuckDB and Arrow docs both say file layout matters;
#   - we want a local measurement, not only vendor guidance.
#
# We benchmark three layout questions on the same logical table:
#   1. Does switching from the current Arrow default layout to explicit zstd
#      + medium row groups materially change size or speed?
#   2. Does Hive-style partitioning by INCM_YR help the year-filter queries
#      that appear throughout the walkthrough?
#   3. Do tiny row groups hurt enough to be worth warning about explicitly?

source("R/00-paths.R")
source("R/helpers.R")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(duckdb)
  library(DBI)
  library(dbplyr)
  library(fs)
  library(glue)
  library(tibble)
})

log_file <- file.path(paths$bench_dir, "parquet-layout-log.parquet")

src_dir <- file.path(paths$parquet_root, "itr_context")
variant_root <- file.path(paths$offline_root, "parquet-layout-bench", "itr_context")
dir_create(variant_root, recurse = TRUE)

variants <- tribble(
  ~variant,                ~dir,                                         ~write_variant, ~compression, ~partitioned, ~row_group_rows,
  "baseline_current",      src_dir,                                      FALSE,          "current",    FALSE,        1048576L,
  "zstd_rg250k",           file.path(variant_root, "zstd_rg250k"),       TRUE,           "zstd",       FALSE,        250000L,
  "year_part_zstd_rg250k", file.path(variant_root, "year_part_zstd_250k"), TRUE,         "zstd",       TRUE,         250000L,
  "year_part_zstd_rg10k",  file.path(variant_root, "year_part_zstd_10k"), TRUE,          "zstd",       TRUE,         10000L
)

log_row <- function(task, variant_row, seconds = NA_real_, size_gb = NA_real_,
                    n_files = NA_integer_, engine = NA_character_,
                    error = NA_character_, n_result = NA_integer_) {
  tibble(
    task = task,
    variant = variant_row$variant,
    engine = engine,
    compression = variant_row$compression,
    partitioned = variant_row$partitioned,
    row_group_rows = variant_row$row_group_rows,
    seconds = seconds,
    size_gb = size_gb,
    n_files = n_files,
    n_result = n_result,
    error = error,
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
}

layout_stats <- function(dir) {
  files <- dir_ls(dir, recurse = TRUE, glob = "*.parquet")
  tibble(
    size_gb = sum(as.numeric(file_info(files)$size)) / 1024^3,
    n_files = length(files)
  )
}

write_variant_dataset <- function(variant_row) {
  if (!variant_row$write_variant) {
    stats <- layout_stats(variant_row$dir)
    row <- log_row("layout", variant_row,
                   size_gb = stats$size_gb,
                   n_files = stats$n_files)
    log_result(row, log_file)
    print(row)
    return(invisible(row))
  }

  if (dir_exists(variant_row$dir)) dir_delete(variant_row$dir)
  dir_create(variant_row$dir, recurse = TRUE)

  message(glue("writing {variant_row$variant}"))
  ds <- open_dataset(src_dir)
  out <- if (isTRUE(variant_row$partitioned)) ds |> group_by(INCM_YR) else ds

  gc(full = TRUE, verbose = FALSE)
  t0 <- Sys.time()
  err <- tryCatch({
    write_dataset(
      out,
      path = variant_row$dir,
      format = "parquet",
      basename_template = "part-{i}.parquet",
      existing_data_behavior = "overwrite",
      max_rows_per_file = 5000000L,
      min_rows_per_group = variant_row$row_group_rows,
      max_rows_per_group = variant_row$row_group_rows,
      compression = variant_row$compression
    )
    NA_character_
  }, error = function(e) conditionMessage(e))
  secs <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  stats <- layout_stats(variant_row$dir)

  row <- log_row(
    task = "write",
    variant_row = variant_row,
    seconds = secs,
    size_gb = stats$size_gb,
    n_files = stats$n_files,
    engine = "build",
    error = err
  )
  log_result(row, log_file)
  print(row)
  invisible(row)
}

sql_string_vec <- function(x) {
  quoted <- vapply(
    x,
    function(path) paste0("'", gsub("'", "''", path, fixed = TRUE), "'"),
    character(1)
  )
  paste0("[", paste(quoted, collapse = ", "), "]")
}

benchmark_one <- function(task, variant_row, engine, run_query) {
  gc(full = TRUE, verbose = FALSE)
  err <- NA_character_
  n_res <- NA_integer_

  try(run_query(), silent = TRUE)  # warm run

  t0 <- Sys.time()
  res <- tryCatch(run_query(), error = function(e) e)
  secs <- as.numeric(difftime(Sys.time(), t0, units = "secs"))

  if (inherits(res, "error")) {
    err <- conditionMessage(res)
  } else if (is.data.frame(res)) {
    n_res <- nrow(res)
  }

  row <- log_row(
    task = task,
    variant_row = variant_row,
    seconds = secs,
    engine = engine,
    error = err,
    n_result = n_res
  )
  log_result(row, log_file)
  print(row)
  invisible(row)
}

for (i in seq_len(nrow(variants))) {
  write_variant_dataset(variants[i, ])
}

con <- dbConnect(duckdb::duckdb())
on.exit(try(dbDisconnect(con, shutdown = TRUE), silent = TRUE), add = TRUE)
dbExecute(con, "SET threads TO 8")
dbExecute(con, "SET memory_limit = '20GB'")

for (i in seq_len(nrow(variants))) {
  variant_row <- variants[i, ]
  files <- dir_ls(variant_row$dir, recurse = TRUE, glob = "*.parquet")

  arrow_ds <- open_dataset(variant_row$dir) |> rename_with(tolower)
  duck_tbl <- tbl(
    con,
    sql(glue("SELECT * FROM read_parquet({sql_string_vec(files)})"))
  ) |> rename_with(tolower)

  benchmark_one(
    task = "year_filter",
    variant_row = variant_row,
    engine = "arrow",
    run_query = function() {
      arrow_ds |>
        filter(incm_yr == 2015L, !is.na(ocptn_grp_cd)) |>
        summarise(n = n()) |>
        collect()
    }
  )

  benchmark_one(
    task = "year_filter",
    variant_row = variant_row,
    engine = "duckdb",
    run_query = function() {
      duck_tbl |>
        filter(incm_yr == 2015L, !is.na(ocptn_grp_cd)) |>
        summarise(n = n()) |>
        collect()
    }
  )

  benchmark_one(
    task = "full_groupby",
    variant_row = variant_row,
    engine = "arrow",
    run_query = function() {
      arrow_ds |>
        count(ocptn_grp_cd) |>
        collect()
    }
  )

  benchmark_one(
    task = "full_groupby",
    variant_row = variant_row,
    engine = "duckdb",
    run_query = function() {
      duck_tbl |>
        count(ocptn_grp_cd) |>
        collect()
    }
  )
}

message("done.")
