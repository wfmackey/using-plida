# Run each (task, backend) pair in a fresh R subprocess.
#
# Fresh subprocesses matter because some backends (dplyr/data.table) load
# the whole parquet into RAM. Running them back-to-back in one process
# would pollute memory measurements and risk OOM cascades.

source("R/00-paths.R")
suppressPackageStartupMessages({ library(fs); library(glue) })

tasks <- c("t1", "t2", "t3")
backends <- c("dplyr", "datatable", "arrow", "duckdb_view", "duckdb_table")

# Skip intentionally-OOM combinations by convention: dplyr/datatable on t3
skip <- list(t3 = c("dplyr", "datatable"))

for (tk in tasks) {
  for (be in backends) {
    if (be %in% (skip[[tk]] %||% character())) {
      message(glue("SKIP {tk}/{be}"))
      next
    }
    message(glue("==> {tk} / {be}"))
    status <- system2("R", c("--no-save", "--no-restore",
                             "-f", "R/03-benchmarks.R",
                             "--args", tk, be),
                      stdout = "", stderr = "")
    message(glue("    exit {status}"))
  }
}
message("all done.")
