# Centralised paths for the project.
# No "big" data lives in the project folder — all intermediate outputs go to
# /Users/willmackey/offline/using-plida-data/.

suppressPackageStartupMessages({
  library(fs)
})

paths <- list(
  csv_root     = "/Users/willmackey/offline/fplida-data/fplida_30m_csv",
  offline_root = "/Users/willmackey/offline/using-plida-data",
  parquet_root = "/Users/willmackey/offline/using-plida-data/parquet",
  duckdb_dir   = "/Users/willmackey/offline/using-plida-data/duckdb",
  duckdb_file  = "/Users/willmackey/offline/using-plida-data/duckdb/plida.duckdb",
  bench_dir    = "/Users/willmackey/Documents/work/anu/misc/using-plida/benchmarks"
)

dir_create(paths$parquet_root)
dir_create(paths$duckdb_dir)
dir_create(paths$bench_dir)

# Convenience: get size of a file or directory in GB
path_size_gb <- function(p) {
  if (is_dir(p)) {
    as.numeric(dir_info(p, recurse = TRUE)$size |> sum()) / 1024^3
  } else {
    as.numeric(file_info(p)$size) / 1024^3
  }
}
