# using-plida

Demo project showing how to use modern R data-engineering tools — **arrow**,
**duckdb** + **dbplyr**, plus **data.table** and **dplyr** for comparison —
on larger-than-memory PLIDA-style administrative microdata.

The source data is the [fplida](https://github.com/wfmackey/fplida) synthetic
PLIDA corpus at 30M individuals (~751 GB of CSV on disk). On this machine
(32 GB RAM) the 44.5 GB MBS 2015 claims file is the canonical stress test.

## Layout

| Path                            | Purpose                                                 |
|---------------------------------|---------------------------------------------------------|
| `index.qmd`                     | Overview and short answer                               |
| `01-the-data.qmd`               | fplida, the subset we use, and the join model           |
| `02-csv-to-parquet.qmd`         | Streaming CSV → parquet with arrow                     |
| `03-duckdb.qmd`                 | Building DuckDB from parquet; views vs materialised     |
| `04-benchmarks.qmd`             | 5 backends × 3 tasks, timings and peak RSS              |
| `05-recommendations.qmd`        | The rule of thumb                                       |
| `R/00-paths.R`                  | Centralised paths                                       |
| `R/helpers.R`                   | Timing and memory-measurement utilities                 |
| `R/01-convert-csv-to-parquet.R` | Arrow-streamed CSV → parquet (itr + mbs)               |
| `R/01b-convert-supplementary.R` | Demo + id-linker spine conversions                      |
| `R/02-build-duckdb.R`           | Build `plida_views.duckdb` and `plida_tables.duckdb`    |
| `R/03-benchmarks.R`             | Benchmark functions, runnable per (task, backend)       |
| `R/04-run-benchmarks.R`         | Driver that runs each pair in a fresh subprocess        |

## Data locations

No "big" data is stored in this repo. The setup expects:

- Source CSVs at `/Users/willmackey/offline/fplida-data/fplida_30m_csv/`
- Intermediate parquet + DuckDB at `/Users/willmackey/offline/using-plida-data/`

## Reproducing

```bash
Rscript R/01-convert-csv-to-parquet.R
Rscript R/01b-convert-supplementary.R

# The ato-pit_itr folder contains four distinct logical tables with
# different schemas. Split them into per-schema subdirs before building
# the DuckDB database:
cd /Users/willmackey/offline/using-plida-data/parquet/ato-pit_itr
mkdir -p ../itr_context ../itr_ded ../itr_inc ../itr_whld
mv itr-20*.parquet itr-madipge-ato-d-context-*.parquet ../itr_context/
mv itr-madipge-ato-d-ded-exp-off-*.parquet ../itr_ded/
mv itr-madipge-ato-d-inc-loss-*.parquet   ../itr_inc/
mv itr-madipge-ato-d-whld-debt-*.parquet  ../itr_whld/
cd - && rmdir /Users/willmackey/offline/using-plida-data/parquet/ato-pit_itr

Rscript R/02-build-duckdb.R
Rscript R/04-run-benchmarks.R
quarto render
open _site/index.html
```

## Headline numbers

| Step                                                  | Wall time | Peak RSS |
|-------------------------------------------------------|-----------|----------|
| MBS 2015 CSV (44.5 GB) → parquet (18.7 GB)            | 122 s     | 1.8 GB   |
| All parquet → DuckDB materialised tables              | ~180 s    | 9.5 GB   |
| Groupby count on 170 M ITR rows (dplyr)               | 34.7 s    | 14.5 GB  |
| Groupby count on 170 M ITR rows (DuckDB, dbplyr)      | 0.16 s    | 0.25 GB  |
| 3-way join + groupby on 372 M MBS rows (dplyr / arrow)| OOM       | —        |
| 3-way join + groupby on 372 M MBS rows (DuckDB, dbplyr)| 3.5 s    | 2.3 GB   |

## The one-line rule

Stream CSV → parquet with arrow, load parquet into DuckDB as tables, and
query everything else through dbplyr. Anything else is a special case.
