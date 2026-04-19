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
| `02-csv-to-parquet.qmd`         | CSV → parquet, with Arrow vs DuckDB comparison         |
| `03-duckdb.qmd`                 | Building DuckDB from parquet; views vs materialised     |
| `04-benchmarks.qmd`             | 5 backends × 3 tasks, timings and peak RSS              |
| `05-recommendations.qmd`        | The rule of thumb                                       |
| `R/00-paths.R`                  | Centralised paths                                       |
| `R/helpers.R`                   | Timing and memory-measurement utilities                 |
| `R/01-convert-csv-to-parquet.R` | DuckDB CSV → parquet (itr + mbs)                       |
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

# Optional: rerun the Arrow vs DuckDB CSV -> parquet benchmark
Rscript R/10-benchmark-csv-backends.R

Rscript R/02-build-duckdb.R
Rscript R/04-run-benchmarks.R
quarto render
open _site/index.html
```

## Headline numbers

| Step                                                   | Wall time | Peak RSS |
|--------------------------------------------------------|-----------|----------|
| CSV → parquet, Arrow (48-51 GB files, mean of 6)       | 130.0 s   | 2.4 GB   |
| CSV → parquet, DuckDB (48-51 GB files, mean of 6)      | 67.3 s    | 2.7 GB   |
| All parquet → DuckDB materialised tables              | ~180 s    | 9.5 GB   |
| Groupby count on 170 M ITR rows (dplyr)               | 34.7 s    | 14.5 GB  |
| Groupby count on 170 M ITR rows (DuckDB, dbplyr)      | 0.16 s    | 0.25 GB  |
| 3-way join + groupby on 372 M MBS rows (dplyr / arrow)| OOM       | —        |
| 3-way join + groupby on 372 M MBS rows (DuckDB, dbplyr)| 3.5 s    | 2.3 GB   |

## The one-line rule

Convert CSV → parquet with DuckDB, load parquet into DuckDB as tables for the
heavy work, and query everything else through dbplyr. Arrow still matters for
parquet-native exploration and interoperability; it is just no longer the best
CSV converter on this machine.
