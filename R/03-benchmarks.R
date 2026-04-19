# Benchmarks: dplyr / data.table / arrow / duckdb on the same tasks.
#
# For each backend we measure wall time and peak RSS on three canonical
# tasks that come up all the time in PLIDA analysis:
#
#   T1 FILTER+COUNT   -- how many ITR records in FY2014-15 have a non-null
#                        occupation code? (single-table scan, heavy filter)
#
#   T2 GROUP+SUMM     -- count rows by occupation group across all ITR years
#                        (group by on a mid-cardinality key, full scan)
#
#   T3 JOIN           -- link demo to MBS 2015 via dhda_spine, then count
#                        MBS services by sex and age group. This is the big
#                        one: the MBS side is 45 GB CSV / ~8 GB parquet and
#                        lives out-of-core on a 32 GB machine.
#
# Each backend is timed in a fresh R subprocess so that memory from a prior
# run cannot pollute the next. Results are appended to a parquet log.
#
# The script is parameterised so we can run a single (task, backend) pair
# via R -f R/03-benchmarks.R --args <task> <backend>

source("R/00-paths.R")
source("R/helpers.R")

suppressPackageStartupMessages({
  library(dplyr); library(dbplyr); library(arrow); library(duckdb); library(DBI)
  library(fs); library(glue)
})

log_file <- file.path(paths$bench_dir, "benchmark-log.parquet")

# Parquet roots
pq <- list(
  itr        = file.path(paths$parquet_root, "itr_context"),
  mbs        = file.path(paths$parquet_root, "dhda-mbs"),
  demo       = file.path(paths$parquet_root, "demo"),
  dhda_spine = file.path(paths$parquet_root, "dhda-spine"),
  ato_spine  = file.path(paths$parquet_root, "ato-spine")
)

# Which ITR year file corresponds to the 'context' (demographics-ish) file
# for FY2014-15 — used by T1 as a concrete filter target.
itr_fy1415_glob <- file.path(pq$itr, "itr-2014-15*.parquet")

# ---- T1: filter+count on ITR FY2014-15 -------------------------------

t1_dplyr <- function() {
  files <- Sys.glob(itr_fy1415_glob)
  # dplyr here means "read the parquet with arrow::read_parquet, then work
  # on the in-memory tibble". This is the baseline that blows up on big data.
  df <- purrr::map_dfr(files, arrow::read_parquet)
  df |>
    filter(!is.na(OCPTN_GRP_CD)) |>
    summarise(n = n())
}

t1_datatable <- function() {
  files <- Sys.glob(itr_fy1415_glob)
  dt <- data.table::rbindlist(lapply(files, arrow::read_parquet))
  data.table::setDT(dt)
  dt[!is.na(OCPTN_GRP_CD), .(n = .N)]
}

t1_arrow <- function() {
  # arrow dataset + dplyr verbs, lazily evaluated then collected.
  # open_dataset does not glob — pass the expanded file list.
  open_dataset(Sys.glob(itr_fy1415_glob), format = "parquet") |>
    filter(!is.na(OCPTN_GRP_CD)) |>
    summarise(n = n()) |>
    collect()
}

t1_duckdb_view <- function() {
  con <- dbConnect(duckdb::duckdb(), dbdir = file.path(paths$duckdb_dir, "plida_views.duckdb"),
                   read_only = TRUE)
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  dbExecute(con, "SET threads TO 8")
  # Views are defined over the whole itr glob, so we filter by filename with
  # a LIKE on the pseudo-column or just re-read the subset directly.
  # Easier: use dbplyr + a raw-SQL filename filter.
  tbl(con, sql(glue(
    "SELECT * FROM read_parquet('{itr_fy1415_glob}')"
  ))) |>
    filter(!is.na(OCPTN_GRP_CD)) |>
    summarise(n = n()) |>
    collect()
}

t1_duckdb_table <- function() {
  # Reads from the materialised table, but filters to FY1415 via INCM_YR.
  con <- dbConnect(duckdb::duckdb(), dbdir = file.path(paths$duckdb_dir, "plida_tables.duckdb"),
                   read_only = TRUE)
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  dbExecute(con, "SET threads TO 8")
  tbl(con, "itr") |>
    filter(INCM_YR == 2014L, !is.na(OCPTN_GRP_CD)) |>
    summarise(n = n()) |>
    collect()
}

# ---- T2: group-by on ITR all years -----------------------------------

t2_dplyr <- function() {
  # Load every ITR parquet into memory — this is the breaking point.
  files <- Sys.glob(file.path(pq$itr, "*.parquet"))
  df <- purrr::map_dfr(files, arrow::read_parquet)
  df |>
    group_by(OCPTN_GRP_CD) |>
    summarise(n = n(), .groups = "drop")
}

t2_datatable <- function() {
  files <- Sys.glob(file.path(pq$itr, "*.parquet"))
  dt <- data.table::rbindlist(lapply(files, arrow::read_parquet), fill = TRUE)
  data.table::setDT(dt)
  dt[, .(n = .N), by = OCPTN_GRP_CD]
}

t2_arrow <- function() {
  open_dataset(pq$itr) |>
    group_by(OCPTN_GRP_CD) |>
    summarise(n = n()) |>
    collect()
}

t2_duckdb_view <- function() {
  con <- dbConnect(duckdb::duckdb(), dbdir = file.path(paths$duckdb_dir, "plida_views.duckdb"),
                   read_only = TRUE)
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  dbExecute(con, "SET threads TO 8")
  tbl(con, "itr") |>
    group_by(OCPTN_GRP_CD) |>
    summarise(n = n()) |>
    collect()
}

t2_duckdb_table <- function() {
  con <- dbConnect(duckdb::duckdb(), dbdir = file.path(paths$duckdb_dir, "plida_tables.duckdb"),
                   read_only = TRUE)
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  dbExecute(con, "SET threads TO 8")
  tbl(con, "itr") |>
    group_by(OCPTN_GRP_CD) |>
    summarise(n = n()) |>
    collect()
}

# ---- T3: demo x dhda_spine x mbs join --------------------------------
#
# For dplyr and data.table we simply refuse to try: loading 45 GB of CSV /
# ~8 GB of parquet plus the spine explodes on a 32 GB machine. We record an
# intentional NA with label "skipped (OOM)" so the recommendations section
# has a concrete comparator.

t3_dplyr <- function() {
  stop("skipped: dplyr on MBS 2015 exceeds 32 GB RAM on this machine")
}
t3_datatable <- function() {
  stop("skipped: data.table on MBS 2015 exceeds 32 GB RAM on this machine")
}

t3_arrow <- function() {
  demo       <- open_dataset(pq$demo)
  dhda_spine <- open_dataset(pq$dhda_spine)
  mbs        <- open_dataset(pq$mbs)

  # Arrow can do streamed joins via the Acero engine (via dplyr verbs on
  # datasets). Services by (sex, age bucket) for MBS 2015.
  demo |>
    select(SPINE_ID, CORE_GENDER, YEAR_OF_BIRTH) |>
    inner_join(
      dhda_spine |> rename(SPINE_ID = spine_id),
      by = "SPINE_ID"
    ) |>
    inner_join(
      mbs |> select(SYNTHETIC_AEUID, NUMSERV, DOS),
      by = "SYNTHETIC_AEUID"
    ) |>
    mutate(age_2015 = 2015L - YEAR_OF_BIRTH) |>
    group_by(CORE_GENDER, age_bucket = as.integer(age_2015 %/% 10L) * 10L) |>
    summarise(services = sum(NUMSERV, na.rm = TRUE),
              rows = n()) |>
    collect()
}

t3_duckdb_view <- function() {
  con <- dbConnect(duckdb::duckdb(), dbdir = file.path(paths$duckdb_dir, "plida_views.duckdb"),
                   read_only = TRUE)
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  dbExecute(con, "SET threads TO 8")
  dbExecute(con, "SET memory_limit = '20GB'")

  demo       <- tbl(con, "demo")
  dhda_spine <- tbl(con, "dhda_spine") |> rename(SPINE_ID = spine_id)
  mbs        <- tbl(con, "mbs") |> select(SYNTHETIC_AEUID, NUMSERV, DOS)

  demo |>
    select(SPINE_ID, CORE_GENDER, YEAR_OF_BIRTH) |>
    inner_join(dhda_spine, by = "SPINE_ID") |>
    inner_join(mbs, by = "SYNTHETIC_AEUID") |>
    mutate(age_2015 = 2015L - YEAR_OF_BIRTH,
           age_bucket = sql("(age_2015 / 10) * 10")) |>
    group_by(CORE_GENDER, age_bucket) |>
    summarise(services = sum(NUMSERV, na.rm = TRUE),
              rows = n(), .groups = "drop") |>
    collect()
}

t3_duckdb_table <- function() {
  con <- dbConnect(duckdb::duckdb(), dbdir = file.path(paths$duckdb_dir, "plida_tables.duckdb"),
                   read_only = TRUE)
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  dbExecute(con, "SET threads TO 8")
  dbExecute(con, "SET memory_limit = '20GB'")

  demo       <- tbl(con, "demo")
  dhda_spine <- tbl(con, "dhda_spine") |> rename(SPINE_ID = spine_id)
  mbs        <- tbl(con, "mbs") |> select(SYNTHETIC_AEUID, NUMSERV, DOS)

  demo |>
    select(SPINE_ID, CORE_GENDER, YEAR_OF_BIRTH) |>
    inner_join(dhda_spine, by = "SPINE_ID") |>
    inner_join(mbs, by = "SYNTHETIC_AEUID") |>
    mutate(age_2015 = 2015L - YEAR_OF_BIRTH,
           age_bucket = sql("(age_2015 / 10) * 10")) |>
    group_by(CORE_GENDER, age_bucket) |>
    summarise(services = sum(NUMSERV, na.rm = TRUE),
              rows = n(), .groups = "drop") |>
    collect()
}

# ---- driver -----------------------------------------------------------

run_one <- function(task, backend) {
  fname <- paste0(task, "_", backend)
  fn <- get(fname)
  gc(full = TRUE, verbose = FALSE)
  rss0 <- rss_gb()
  t0 <- Sys.time()
  res <- tryCatch(fn(),
                  error = function(e) list(.error = conditionMessage(e)))
  t1 <- Sys.time()
  rss1 <- rss_gb()
  err <- if (!is.data.frame(res) && is.list(res) && !is.null(res$.error)) res$.error else NA_character_
  n <- if (is.data.frame(res)) nrow(res) else NA_integer_
  tibble(
    task = task, approach = backend,
    seconds = as.numeric(difftime(t1, t0, units = "secs")),
    rss_before_gb = rss0, rss_after_gb = rss1,
    rss_delta_gb  = rss1 - rss0,
    n_result = n,
    error = err,
    timestamp = format(t1, "%Y-%m-%d %H:%M:%S")
  )
}

# Command-line args: <task> <backend>
args <- commandArgs(trailingOnly = TRUE)
if (length(args) >= 2) {
  row <- run_one(args[[1]], args[[2]])
  print(row)
  log_result(row, log_file)
} else {
  message("usage: Rscript R/03-benchmarks.R <task> <backend>")
  message("  task    in {t1, t2, t3}")
  message("  backend in {dplyr, datatable, arrow, duckdb_view, duckdb_table}")
}
