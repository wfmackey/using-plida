# Supplementary conversions: the demographic table + id-linker spines.
#
# The original plan used _system/base-spine.csv (11 GB) as the universal
# spine. That file disappeared between runs (fplida_30m_csv/_system was
# regenerated). We pivot to a more realistic PLIDA-style join model:
#
#   demo        = abs-core/plidage-core-demog-cb-c21-2006-latest.csv
#                 keyed by SPINE_ID — birth year, gender, etc.
#   abs_spine   = abs-core/abs-spine.csv
#                 maps SPINE_ID -> ABS SYNTHETIC_AEUID
#   ato_spine   = ato-pit_itr/ato-spine.csv
#                 maps SPINE_ID -> ATO SYNTHETIC_AEUID
#   dhda_spine  = dhda-mbs/dhda-spine.csv
#                 maps SPINE_ID -> DHDA SYNTHETIC_AEUID
#
# Realistic join chain for a "how much MBS spending does each person get in
# their first income year after age 25" question:
#
#   demo [SPINE_ID]
#     --- ato_spine [spine_id, SYNTHETIC_AEUID] --- itr [SYNTHETIC_AEUID]
#     --- dhda_spine [spine_id, SYNTHETIC_AEUID] --- mbs [SYNTHETIC_AEUID]

source("R/00-paths.R")
source("R/helpers.R")

suppressPackageStartupMessages({
  library(arrow); library(fs); library(dplyr); library(glue)
})

log_file <- file.path(paths$bench_dir, "convert-log.parquet")

convert_one <- function(csv_path, out_dir, label, basename_prefix) {
  dir_create(out_dir)
  message(glue("  converting {path_file(csv_path)} ({round(path_size_gb(csv_path),2)} GB)"))
  gc(full = TRUE, verbose = FALSE)
  rss0 <- rss_gb(); t0 <- Sys.time()
  res <- tryCatch({
    ds <- open_csv_dataset(csv_path)
    write_dataset(
      ds, path = out_dir, format = "parquet",
      basename_template = glue("{basename_prefix}-{{i}}.parquet"),
      max_rows_per_file = 5e6,
      existing_data_behavior = "overwrite"
    )
    NULL
  }, error = function(e) conditionMessage(e))
  t1 <- Sys.time(); rss1 <- rss_gb()
  tibble(
    task = "convert", approach = "arrow-stream", label = label,
    file = path_file(csv_path),
    csv_gb = path_size_gb(csv_path),
    parquet_gb = tryCatch(sum(as.numeric(dir_info(out_dir, glob = glue("*{basename_prefix}*.parquet"))$size))/1024^3,
                          error = function(e) NA_real_),
    seconds = as.numeric(difftime(t1, t0, units = "secs")),
    rss_peak_gb = max(rss0, rss1, na.rm = TRUE),
    skipped = FALSE,
    error = if (is.null(res)) NA_character_ else res
  )
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
