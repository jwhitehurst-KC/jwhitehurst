library(arrow)
library(DBI)
library(odbc)
library(dplyr)
library(fs)
library(progress)
library(bit64)
library(cli)
library(future.apply)
library(parallelly)
library(memuse)

# ============================================================
# START TIMER
# ============================================================

start_time <- Sys.time()

# ============================================================
# GLOBAL CONFIG
# ============================================================

parquet_dir <- "//dphcifs/APDE-CDIP/Mcaid-Mcare/testing/v2_hhsaw"   # directory
schema_name <- "jwhitehurst"                        # schema for destination table
table_name  <- "final_apcd_elig_month"            # destination table name

cli_blue <- function(msg) cli::cli_inform(cli::col_blue(msg))


# ============================================================
# DYNAMIC BATCH SIZE OPTIMIZER (memory + throughput)
# ============================================================

optimize_batch_size <- function(prev_batch_size = 250000,
                                last_duration = NULL,
                                target_secs = 1.0) {

  # If no timing yet, keep initial batch size
  if (is.null(last_duration)) return(prev_batch_size)

  # Throughput adjustment
  if (last_duration < target_secs) {
    new_batch <- prev_batch_size * 1.25
  } else {
    new_batch <- prev_batch_size * 0.80
  }

  # Memory-aware cap
  mem <- memuse::Sys.meminfo()

  # Convert "free" memory S4 object → numeric bytes
  avail_bytes <- as.numeric(mem$free, units = "bytes")

  bytes_per_row <- 800  # heuristic average
  max_rows <- as.integer((avail_bytes * 0.10) / bytes_per_row)

  new_batch <- max(50000, min(new_batch, max_rows, 2000000))
  as.integer(new_batch)
}


# ============================================================
# TYPE CONVERSION LOGIC
# ============================================================

convert_types_for_sql <- function(df) {

  for (col in names(df)) {
    val <- df[[col]]
    cls <- class(val)[1]

    if (cls == "logical") df[[col]] <- as.integer(val)
    if (cls == "numeric" && all(val %% 1 == 0, na.rm = TRUE)) df[[col]] <- as.integer64(val)
    if (cls == "factor") df[[col]] <- as.character(val)
    if (cls == "character") df[[col]] <- trimws(val)
    if (cls == "POSIXct") df[[col]] <- format(val, "%Y-%m-%d %H:%M:%S")
    if (cls == "Date") df[[col]] <- format(val, "%Y-%m-%d")

  }

  df
}

# ============================================================
# VALIDATION + FIXUPS
# ============================================================

validate_and_fix_types <- function(df, con, schema, table) {

  sql_info <- dbGetQuery(con, sprintf("
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'
    ", schema, table))

  if (nrow(sql_info) == 0) {
    cli_blue("Table not found; skipping validation.")
    return(df)
  }

  df <- convert_types_for_sql(df)

  for (col in names(df)) {
    sql_col <- sql_info[sql_info$COLUMN_NAME == col, ]

    if (nrow(sql_col) == 0) next

    sql_len <- sql_col$CHARACTER_MAXIMUM_LENGTH

    if (is.character(df[[col]]) && !is.na(sql_len) && sql_len > 0) {
      too_long <- nchar(df[[col]]) > sql_len
      if (any(too_long, na.rm = TRUE)) {
        cli_blue(sprintf("Truncating '%s' to %d characters.", col, sql_len))
        df[[col]] <- substr(df[[col]], 1, sql_len)
      }
    }
  }

  df
}

# ============================================================
# TABLE CREATION FUNCTION
# ============================================================

create_table_if_missing <- function(con, df, schema, table) {

  exists <- dbExistsTable(con, Id(schema = schema, table = table))

  if (!exists) {
    cli_blue(sprintf("Creating table %s.%s ...", schema, table))
    dbCreateTable(con, Id(schema = schema, table = table), df)
  }

  invisible(TRUE)
}

# ============================================================
# SAFE SQL APPEND WITH RETRIES (handles deadlocks, timeouts)
# ============================================================

safe_append <- function(con, df, schema, table, retries = 5) {

  attempt <- 1

  repeat {
    try({
      dbAppendTable(
        conn = con,
        name = Id(schema = schema, table = table),
        value = df,
        row.names = NULL
      )
      return(TRUE)
    }, silent = TRUE)

    err <- geterrmessage()

    # SQL Server deadlock: 1205
    if (attempt <= retries &&
        (grepl("1205", err) || grepl("deadlock", err, ignore.case = TRUE))) {

      cli_blue(sprintf("Deadlock detected. Retrying (%d/%d)...",
                       attempt, retries))
      Sys.sleep(runif(1, 0.5, 1.5))
      attempt <- attempt + 1
      next
    }

    stop("SQL append failed: ", err)
  }
}

# ============================================================
# WORKER FUNCTION — PROCESSES ONE PARQUET FILE
# ============================================================

process_parquet_file <- function(file, schema, table, conn_str) {

  cli_blue(sprintf("Worker processing file: %s", file))

  # Each worker must open its own DB connection
  con <- DBI::dbConnect(odbc(),
    Driver   = conn_str$Driver,
    Server   = conn_str$Server,
    Database = conn_str$Database,
    Encrypt  = conn_str$Encrypt,
    TrustServerCertificate = conn_str$TrustServerCertificate,
    Authentication = conn_str$Authentication
  )

  # Open dataset and prepare scanner
  dataset <- open_dataset(file)
  batch_size <- 250000
  last_duration <- NULL

  scanner <- Scanner$create(dataset, batch_size = batch_size)
  batches <- scanner$ToRecordBatchReader()

  first_chunk <- TRUE

  repeat {
    batch <- batches$read_next_batch()
    if (is.null(batch)) break

    df <- as.data.frame(batch)

    # First batch → ensure table exists
    if (first_chunk) {
      create_table_if_missing(con, df, schema, table)
      first_chunk <- FALSE
    }

    # Validate + fix types
    df <- validate_and_fix_types(df, con, schema, table)

    # Measure performance
    t0 <- Sys.time()
    safe_append(con, df, schema, table)
    t1 <- Sys.time()

    last_duration <- as.numeric(difftime(t1, t0, units = "secs"))

    # Adapt batch size dynamically
    batch_size <- optimize_batch_size(
      prev_batch_size = batch_size,
      last_duration   = last_duration,
      target_secs     = 1.0
    )

    # Update scanner with new batch size
    scanner <- Scanner$create(dataset, batch_size = batch_size)
    batches <- scanner$ToRecordBatchReader()

    cli_blue(sprintf("File %s: new batch size = %s rows",
                     basename(file), batch_size))

    rm(df)
    gc()
  }

  cli_blue(sprintf("Finished file: %s", basename(file)))
  DBI::dbDisconnect(con)

  return(TRUE)
}

# ============================================================
# PARALLEL ORCHESTRATOR — DISTRIBUTE FILES ACROSS WORKERS
# ============================================================

cli_blue("Scanning for parquet files...")

parquet_files <- dir_ls(parquet_dir, regexp = "\\.parquet$", recurse = TRUE)

if (length(parquet_files) == 0) {
  stop("No Parquet files found in: ", parquet_dir)
}

cli_blue(sprintf("Discovered %d parquet files.", length(parquet_files)))

# ============================================================
# ESTIMATE TOTAL ROWS FOR GLOBAL PROGRESS BAR
# ============================================================

cli_blue("Estimating total rows (fast scan)...")

total_rows_est <- 0
for (file in parquet_files) {
  ds <- open_dataset(file)
  total_rows_est <- total_rows_est + ds$num_rows
}

cli_blue(sprintf("Estimated total rows: %s", format(total_rows_est, big.mark=",")))

global_pb <- progress_bar$new(
  total = total_rows_est,
  format = cli::col_blue("Overall import [:bar] :percent ETA: :eta Rows: :current/:total")
)

# ============================================================
# PARALLEL PLAN
# ============================================================

available_cores <- parallelly::availableCores()
cli_blue(sprintf("Using %d cores for parallel import.", available_cores))

plan(multisession, workers = available_cores)

# ============================================================
# WRAPPER FOR WORKERS TO REPORT PROGRESS UPWARD
# ============================================================

parallel_worker_wrapper <- function(file, schema, table, conn_str) {

  ds <- open_dataset(file)

  # Track rows processed in this file
  local_count <- 0

  # Use the worker function from Section 2:
  process_parquet_file_with_progress <- function() {

    dataset <- open_dataset(file)
    batch_size <- 250000
    last_duration <- NULL

    scanner <- Scanner$create(dataset, batch_size = batch_size)
    batches <- scanner$ToRecordBatchReader()

    first_chunk <- TRUE

    # Worker opens its own connection
    con <- DBI::dbConnect(odbc(),
      Driver   = conn_str$Driver,
      Server   = conn_str$Server,
      Database = conn_str$Database,
      Encrypt  = conn_str$Encrypt,
      TrustServerCertificate = conn_str$TrustServerCertificate,
      Authentication = conn_str$Authentication
    )

    repeat {
      batch <- batches$read_next_batch()
      if (is.null(batch)) break

      df <- as.data.frame(batch)
      local_count <<- local_count + nrow(df)

      if (first_chunk) {
        create_table_if_missing(con, df, schema, table)
        first_chunk <- FALSE
      }

      df <- validate_and_fix_types(df, con, schema, table)

      t0 <- Sys.time()
      safe_append(con, df, schema, table)
      t1 <- Sys.time()

      last_duration <- as.numeric(difftime(t1, t0, units="secs"))

      # Tune batch size
      batch_size <- optimize_batch_size(
        prev_batch_size = batch_size,
        last_duration   = last_duration,
        target_secs     = 1.0
      )

      scanner <- Scanner$create(dataset, batch_size = batch_size)
      batches <- scanner$ToRecordBatchReader()

      # RETURN local progress so parent can update the global bar
      global_pb$tick(nrow(df))

      rm(df)
      gc()
    }

    DBI::dbDisconnect(con)
    cli_blue(sprintf("Worker finished file %s (%s rows)",
                     basename(file), format(local_count, big.mark=",")))

    TRUE
  }

  process_parquet_file_with_progress()
}

# ============================================================
# CONNECTION STRING FOR WORKERS
# ============================================================

conn_str <- list(
  Driver = "ODBC Driver 17 for SQL Server",
  Server = "kcitazrhpasqlprp16.azds.kingcounty.gov",
  Database = "hhs_analytics_workspace",
  Encrypt = "yes",
  TrustServerCertificate = "yes",
  Authentication = "ActiveDirectoryIntegrated"
)

cli_blue("Starting parallel imports...")

# ============================================================
# FINAL PARALLEL EXECUTION
# ============================================================

cli_blue("Launching parallel import tasks...")

# Run workers on parquet files
future_lapply(
  parquet_files,
  parallel_worker_wrapper,
  schema   = schema_name,
  table    = table_name,
  conn_str = conn_str,
  future.packages = c(
    "arrow", "DBI", "odbc", "dplyr", "bit64",
    "cli", "fs", "parallelly", "progress"
  )
)

cli_blue("All parallel tasks completed successfully!")

# Shut down workers
plan(sequential)

# ============================================================
# END TIMER — PRINT TOTAL DURATION
# ============================================================

end_time <- Sys.time()
duration <- end_time - start_time

total_minutes <- as.numeric(duration, units = "mins")
hours <- floor(total_minutes / 60)
minutes <- round(total_minutes %% 60)

cli_blue(sprintf(
  "Total duration %d hour(s) and %d minute(s).",
  hours, minutes
))