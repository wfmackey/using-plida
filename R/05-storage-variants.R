# Build storage-layout variants of the MBS table to measure the effect of
# sorting, data types, and constraints on query performance in DuckDB.
#
# The DuckDB docs are unambiguous about a few things:
#
#   - DuckDB does not use B-tree indexes for analytical queries. It uses
#     *zonemaps* (min/max per ~122,880-row row group) on every column
#     automatically.
#
#   - The more ordered a column is, the more selective its zonemap becomes.
#     Sorted data can dramatically accelerate point lookups and, more
#     importantly, joins where a filter on the build side is pushed to the
#     probe side as a min/max / bloom filter.
#
#   - PRIMARY KEY and UNIQUE constraints create ART indexes. ART indexes
#     help highly selective point lookups but do nothing for joins or
#     aggregations, and they slow ingest by ~4x. They are for integrity,
#     not performance.
#
#   - Integer joins are materially faster than string joins on the same
#     logical key.
#
# So the four variants are:
#
#   V0 baseline   — CREATE TABLE AS SELECT * FROM base.mbs
#                   (no ordering — parquet insertion order)
#
#   V1 sorted     — ORDER BY SYNTHETIC_AEUID (the join key)
#
#   V2 intkey     — hash(SYNTHETIC_AEUID) AS aeuid_hash, ORDER BY aeuid_hash
#                   (test the "int join is faster than string join" claim)
#
#   V3 spine_pk   — dhda_spine with PRIMARY KEY on spine_id
#
# The experiment runs four tasks on each applicable variant:
#
#   A  POINT    — one person's MBS history (single SYNTHETIC_AEUID)
#   B  NARROW   — demo filtered to birth year 1970, joined through
#                 dhda_spine, then to mbs (~0.5M person keys)
#   C  FULL3    — full three-way join with groupby (same as T3 in the
#                 main benchmark page, included for parity)
#   D  INTKEY   — task B run against V2 using the integer key

source("R/00-paths.R")
source("R/helpers.R")

suppressPackageStartupMessages({
  library(duckdb); library(DBI); library(dplyr); library(dbplyr)
  library(fs); library(glue); library(tibble); library(arrow)
})

variants_db <- file.path(paths$duckdb_dir, "plida_variants.duckdb")
base_db     <- file.path(paths$duckdb_dir, "plida_tables.duckdb")

log_file    <- file.path(paths$bench_dir, "storage-variant-log.parquet")

# ---- (1) Build variants ------------------------------------------------

if (file_exists(variants_db)) file_delete(variants_db)
con <- dbConnect(duckdb::duckdb(), dbdir = variants_db, read_only = FALSE)
on.exit(try(dbDisconnect(con, shutdown = TRUE), silent = TRUE), add = TRUE)

dbExecute(con, "SET threads TO 8")
dbExecute(con, "SET memory_limit = '20GB'")
dbExecute(con, glue("ATTACH '{base_db}' AS base (READ_ONLY)"))

time_sql <- function(label, sql) {
  message(glue("  -> {label}"))
  gc(full = TRUE, verbose = FALSE)
  rss0 <- rss_gb()
  t0 <- Sys.time()
  dbExecute(con, sql)
  t1 <- Sys.time()
  rss1 <- rss_gb()
  row <- tibble(
    task = "build", approach = label,
    seconds = as.numeric(difftime(t1, t0, units = "secs")),
    rss_after_gb = rss1,
    rss_delta_gb = rss1 - rss0,
    db_size_gb = path_size_gb(variants_db),
    error = NA_character_,
    timestamp = format(t1, "%Y-%m-%d %H:%M:%S")
  )
  print(row)
  log_result(row, log_file)
  invisible(row)
}

# small tables: we make a single copy and reuse
time_sql("copy demo",       "CREATE TABLE demo AS SELECT * FROM base.demo")
time_sql("copy dhda_spine", "CREATE TABLE dhda_spine AS SELECT * FROM base.dhda_spine")

# V0: baseline MBS (unsorted; same layout as plida_tables.duckdb)
time_sql("V0 mbs_baseline", "CREATE TABLE mbs_baseline AS SELECT * FROM base.mbs")

# V1: MBS sorted by SYNTHETIC_AEUID
time_sql(
  "V1 mbs_sorted",
  "CREATE TABLE mbs_sorted AS SELECT * FROM base.mbs ORDER BY SYNTHETIC_AEUID"
)

# V2: MBS with int hash key, sorted by it
time_sql(
  "V2 mbs_intkey",
  "CREATE TABLE mbs_intkey AS
   SELECT hash(SYNTHETIC_AEUID) AS aeuid_hash, * FROM base.mbs
   ORDER BY aeuid_hash"
)

# V3: dhda_spine variants for the INTKEY task — one with the matching hash
# column, and a separate one with a PRIMARY KEY on spine_id so we can see
# whether the PK makes any difference.
time_sql(
  "V3 dhda_spine_intkey",
  "CREATE TABLE dhda_spine_intkey AS
   SELECT hash(SYNTHETIC_AEUID) AS aeuid_hash, * FROM base.dhda_spine"
)

time_sql(
  "V3 dhda_spine_pk",
  "CREATE TABLE dhda_spine_pk (spine_id VARCHAR PRIMARY KEY,
                                SYNTHETIC_AEUID VARCHAR)"
)
time_sql(
  "V3 dhda_spine_pk fill",
  "INSERT INTO dhda_spine_pk SELECT spine_id, SYNTHETIC_AEUID FROM base.dhda_spine"
)

dbExecute(con, "DETACH base")

# ---- (2) Pick a point-lookup target -----------------------------------

# A well-represented person: pick one with many claims so the result is
# visible but not the pathological max.
target_aeuid <- dbGetQuery(con, "
  SELECT SYNTHETIC_AEUID, COUNT(*) AS n
  FROM mbs_baseline
  WHERE SYNTHETIC_AEUID IS NOT NULL
  GROUP BY SYNTHETIC_AEUID
  HAVING COUNT(*) BETWEEN 25 AND 40
  LIMIT 1
")$SYNTHETIC_AEUID
message(glue("point-lookup target: {target_aeuid}"))

# ---- (3) Task runner --------------------------------------------------

time_query <- function(task, approach, query) {
  gc(full = TRUE, verbose = FALSE)
  rss0 <- rss_gb()
  t0 <- Sys.time()
  res <- tryCatch(dbGetQuery(con, query),
                  error = function(e) data.frame(.error = conditionMessage(e)))
  t1 <- Sys.time()
  rss1 <- rss_gb()
  err <- if (".error" %in% names(res)) res$.error[[1]] else NA_character_
  row <- tibble(
    task = task, approach = approach,
    seconds = as.numeric(difftime(t1, t0, units = "secs")),
    rss_after_gb = rss1,
    rss_delta_gb = rss1 - rss0,
    db_size_gb = NA_real_,
    error = err,
    timestamp = format(t1, "%Y-%m-%d %H:%M:%S"),
    n_result = if (is.null(err)) nrow(res) else NA_integer_
  )
  print(row)
  log_result(row, log_file)
  invisible(row)
}

# ---- Task A: POINT ----------------------------------------------------

for (variant in c("mbs_baseline", "mbs_sorted")) {
  time_query(
    task = "point",
    approach = variant,
    query = glue(
      "SELECT COUNT(*) AS n, SUM(NUMSERV) AS services
       FROM {variant}
       WHERE SYNTHETIC_AEUID = '{target_aeuid}'"
    )
  )
}

# ---- Task B: NARROW (birth year 1970) --------------------------------

narrow_sql_template <- "
  SELECT COUNT(*) AS rows, SUM(NUMSERV) AS services
  FROM demo d
  JOIN dhda_spine ds ON d.SPINE_ID = ds.spine_id
  JOIN {mbs_tbl} m    ON ds.SYNTHETIC_AEUID = m.SYNTHETIC_AEUID
  WHERE d.YEAR_OF_BIRTH = 1970
"

for (variant in c("mbs_baseline", "mbs_sorted")) {
  time_query("narrow", variant, glue(narrow_sql_template, mbs_tbl = variant))
}

# ---- Task C: FULL3 ----------------------------------------------------

full_sql_template <- "
  SELECT d.CORE_GENDER,
         ((2015 - d.YEAR_OF_BIRTH) / 10) * 10 AS age_bucket,
         SUM(m.NUMSERV) AS services,
         COUNT(*) AS rows
  FROM demo d
  JOIN dhda_spine ds ON d.SPINE_ID = ds.spine_id
  JOIN {mbs_tbl} m    ON ds.SYNTHETIC_AEUID = m.SYNTHETIC_AEUID
  GROUP BY d.CORE_GENDER, age_bucket
  ORDER BY d.CORE_GENDER, age_bucket
"

for (variant in c("mbs_baseline", "mbs_sorted")) {
  time_query("full3", variant, glue(full_sql_template, mbs_tbl = variant))
}

# ---- Task D: INTKEY narrow join (hash-key join) ----------------------

intkey_narrow_sql <- "
  SELECT COUNT(*) AS rows, SUM(m.NUMSERV) AS services
  FROM demo d
  JOIN dhda_spine_intkey ds ON d.SPINE_ID = ds.spine_id
  JOIN mbs_intkey m         ON ds.aeuid_hash = m.aeuid_hash
  WHERE d.YEAR_OF_BIRTH = 1970
"
time_query("intkey_narrow", "mbs_intkey", intkey_narrow_sql)

message("done.")
