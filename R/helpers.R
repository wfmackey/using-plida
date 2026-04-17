# Timing and memory helpers for benchmarks.
#
# We deliberately avoid packages like {bench} that try to re-run code many
# times: each iteration on a 10+ GB file would take minutes, not microseconds.
# Instead we time single executions, measure peak RSS via ps(1), and save
# results to parquet so that .qmd files can read them back without re-running.

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
  library(arrow)
  library(fs)
})

# Peak resident-set size (GB) of the current R process, sampled from ps.
# Returns a single number at the time of call.
rss_gb <- function(pid = Sys.getpid()) {
  out <- tryCatch(
    system2("ps", c("-o", "rss=", "-p", pid), stdout = TRUE),
    error = function(e) NA_character_
  )
  if (length(out) == 0 || is.na(out)) return(NA_real_)
  as.numeric(trimws(out)) / 1024 / 1024
}

# Time an expression and return a one-row tibble with wall time (s), peak RSS
# (GB) sampled before/after, and a user-supplied label.
time_it <- function(expr, label, approach = NA_character_, task = NA_character_) {
  gc(full = TRUE, verbose = FALSE)
  rss_before <- rss_gb()
  t0 <- Sys.time()
  val <- force(expr)
  t1 <- Sys.time()
  rss_after <- rss_gb()
  tibble(
    task      = task,
    approach  = approach,
    label     = label,
    seconds   = as.numeric(difftime(t1, t0, units = "secs")),
    rss_before_gb = rss_before,
    rss_after_gb  = rss_after,
    rss_delta_gb  = rss_after - rss_before,
    n_result  = if (is.data.frame(val)) nrow(val) else NA_integer_,
    timestamp = format(t1, "%Y-%m-%d %H:%M:%S")
  )
}

# Append a row (or rows) to a parquet log; create if missing.
log_result <- function(row, file) {
  dir_create(path_dir(file))
  if (file_exists(file)) {
    prior <- read_parquet(file)
    out <- bind_rows(prior, row)
  } else {
    out <- row
  }
  write_parquet(out, file)
  invisible(out)
}
