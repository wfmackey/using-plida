# Run the storage-variant benchmark tasks against the pre-built
# plida_variants.duckdb. We also finish the one build step that failed in
# 05-storage-variants.R (the PRIMARY KEY spine table needed a NULL filter
# because ~0.4% of dhda_spine rows have a missing spine_id).

source("R/00-paths.R")
source("R/helpers.R")

suppressPackageStartupMessages({
  library(duckdb); library(DBI); library(dplyr); library(dbplyr)
  library(fs); library(glue); library(tibble); library(arrow)
})

variants_db <- file.path(paths$duckdb_dir, "plida_variants.duckdb")
log_file    <- file.path(paths$bench_dir, "storage-variant-log.parquet")

con <- dbConnect(duckdb::duckdb(), dbdir = variants_db, read_only = FALSE)
on.exit(try(dbDisconnect(con, shutdown = TRUE), silent = TRUE), add = TRUE)

dbExecute(con, "SET threads TO 8")
dbExecute(con, "SET memory_limit = '20GB'")

# ---- finish the PK build with NULL filter ----------------------------

n_existing <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM dhda_spine_pk")$n
if (n_existing == 0) {
  message("  -> V3 dhda_spine_pk fill (NULL-filtered)")
  gc(full = TRUE, verbose = FALSE)
  rss0 <- rss_gb()
  t0 <- Sys.time()
  dbExecute(con, "
    INSERT INTO dhda_spine_pk
    SELECT spine_id, SYNTHETIC_AEUID
    FROM dhda_spine
    WHERE spine_id IS NOT NULL
  ")
  t1 <- Sys.time()
  rss1 <- rss_gb()
  log_result(tibble(
    task = "build", approach = "V3 dhda_spine_pk fill",
    seconds = as.numeric(difftime(t1, t0, units = "secs")),
    rss_after_gb = rss1, rss_delta_gb = rss1 - rss0,
    db_size_gb = path_size_gb(variants_db),
    error = NA_character_,
    timestamp = format(t1, "%Y-%m-%d %H:%M:%S")
  ), log_file)
}

# ---- pick a point-lookup target --------------------------------------

target_aeuid <- dbGetQuery(con, "
  SELECT SYNTHETIC_AEUID, COUNT(*) AS n
  FROM mbs_baseline
  WHERE SYNTHETIC_AEUID IS NOT NULL
  GROUP BY SYNTHETIC_AEUID
  HAVING COUNT(*) BETWEEN 25 AND 40
  LIMIT 1
")$SYNTHETIC_AEUID
message(glue("point-lookup target: {target_aeuid}"))

# ---- task runner -----------------------------------------------------

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
    rss_after_gb = rss1, rss_delta_gb = rss1 - rss0,
    db_size_gb = NA_real_,
    error = err,
    timestamp = format(t1, "%Y-%m-%d %H:%M:%S"),
    n_result = if (is.na(err)) nrow(res) else NA_integer_
  )
  print(row)
  log_result(row, log_file)
  invisible(row)
}

# Each task is run TWICE and we keep the second (cache-warm) number for
# the comparative analysis — on a repeatable-production workload the warm
# number is what you will actually see.

time_query_warm <- function(task, approach, query) {
  # warm-up
  try(dbGetQuery(con, query), silent = TRUE)
  time_query(task, approach, query)
}

# ---- Task A: POINT ---------------------------------------------------

for (variant in c("mbs_baseline", "mbs_sorted", "mbs_intkey")) {
  key_col <- if (variant == "mbs_intkey") "aeuid_hash" else "SYNTHETIC_AEUID"
  target_expr <- if (variant == "mbs_intkey") {
    glue("hash('{target_aeuid}')")
  } else {
    glue("'{target_aeuid}'")
  }
  time_query_warm(
    "point", variant,
    glue("SELECT COUNT(*) AS n, SUM(NUMSERV) AS services
          FROM {variant} WHERE {key_col} = {target_expr}")
  )
}

# ---- Task B: NARROW (birth year 1970) ------------------------------

narrow_sql_template <- "
  SELECT COUNT(*) AS rows, SUM(m.NUMSERV) AS services
  FROM demo d
  JOIN dhda_spine ds ON d.SPINE_ID = ds.spine_id
  JOIN {mbs_tbl} m    ON ds.SYNTHETIC_AEUID = m.SYNTHETIC_AEUID
  WHERE d.YEAR_OF_BIRTH = 1970
"

for (variant in c("mbs_baseline", "mbs_sorted")) {
  time_query_warm("narrow", variant, glue(narrow_sql_template, mbs_tbl = variant))
}

# ---- Task C: FULL3 --------------------------------------------------

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
  time_query_warm("full3", variant, glue(full_sql_template, mbs_tbl = variant))
}

# ---- Task D: INTKEY narrow join -------------------------------------

intkey_narrow_sql <- "
  SELECT COUNT(*) AS rows, SUM(m.NUMSERV) AS services
  FROM demo d
  JOIN dhda_spine_intkey ds ON d.SPINE_ID = ds.spine_id
  JOIN mbs_intkey m         ON ds.aeuid_hash = m.aeuid_hash
  WHERE d.YEAR_OF_BIRTH = 1970
"
time_query_warm("intkey_narrow", "mbs_intkey", intkey_narrow_sql)

message("done.")
