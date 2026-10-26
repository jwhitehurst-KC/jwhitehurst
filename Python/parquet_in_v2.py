# ==============================================
# parquet_in_v2.py
# ==============================================
# Parallel Parquet → SQL Server Importer
# - pyodbc for SQL inserts
# - pyarrow for chunked parquet reading
# - rich for colored logs + progress bars
# - dynamic batch-size tuning
# - automatic type conversion
# - Windows‑safe multiprocessing
# - deadlock‑safe retry logic

import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime

import numpy as np
import pandas as pd
import psutil
import pyarrow.dataset as ds
import pyodbc
from rich.console import Console
from rich.progress import BarColumn, Progress, TimeRemainingColumn

console = Console()

# ============================================================
# CONFIGURATION
# ============================================================

PARQUET_DIR = r"//dphcifs/APDE-CDIP/Mcaid-Mcare/testing/v2_hhsaw"
SERVER = "kcitazrhpasqlprp16.azds.kingcounty.gov"
DATABASE = "hhs_analytics_workspace"
SCHEMA = "jwhitehurst"
TABLE = "parquet_test_pyodbc"
DRIVER = "{ODBC Driver 17 for SQL Server}"

# ============================================================
# BLUE LOGGING
# ============================================================

def log(msg):
    console.print(f"[bold blue]{msg}[/bold blue]")

# ============================================================
# SQL CONNECTION FACTORY
# ============================================================

def get_connection():
    conn_str = (
        f"DRIVER={DRIVER};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
        f"Authentication=ActiveDirectoryIntegrated"
    )
    return pyodbc.connect(conn_str, autocommit=True)

# ============================================================
# DYNAMIC BATCH SIZE OPTIMIZER
# ============================================================

def optimize_batch_size(prev_batch_size, last_duration, target_seconds=1.0):
    if last_duration is None:
        return prev_batch_size

    if last_duration < target_seconds:
        new_batch = int(prev_batch_size * 1.25)
    else:
        new_batch = int(prev_batch_size * 0.80)

    avail_bytes = psutil.virtual_memory().available
    bytes_per_row = 800
    max_rows = int((avail_bytes * 0.10) / bytes_per_row)

    new_batch = max(50_000, min(new_batch, max_rows, 2_000_000))
    return new_batch

# ============================================================
# TYPE CONVERSION RULES
# ============================================================

def convert_types_for_sql(df: pd.DataFrame, sql_colinfo: dict):
    for col in df.columns:
        if col not in sql_colinfo:
            continue

        series = df[col]
        sql_type, maxlen = sql_colinfo[col]
        dtype = str(series.dtype).lower()

        if "bool" in dtype:
            df[col] = series.astype("Int64").fillna(0)

        elif "int" in dtype:
            df[col] = series.astype("Int64")

        elif "float" in dtype:
            df[col] = series.astype(float)

        elif "string" in dtype or "object" in dtype:
            df[col] = series.astype(str).str.strip()
            if maxlen and maxlen > 0:
                df[col] = df[col].str.slice(0, maxlen)

        elif "date" in dtype:
            df[col] = pd.to_datetime(series).dt.strftime("%Y-%m-%d")

        elif "timestamp" in dtype:
            df[col] = pd.to_datetime(series).dt.strftime("%Y-%m-%d %H:%M:%S")

        elif "decimal" in dtype:
            df[col] = series.astype(str)

        else:
            df[col] = series.astype(str)

    return df

# ============================================================
# CONVERT VALUES TO PYTHON-NATIVE TYPES
# ============================================================

def to_python_native(df):
    out_df = {}
    for col in df.columns:
        series = df[col]
        converted = []
        for v in series:
            if hasattr(v, "item"):
                v = v.item()
            elif hasattr(v, "to_pydatetime"):
                v = v.to_pydatetime()
            converted.append(v)
        out_df[col] = converted
    return pd.DataFrame(out_df)

# ============================================================
# SQL RETRY LOGIC
# ============================================================

def safe_insert(cursor, query, params, retries=5):
    attempt = 1
    while True:
        try:
            cursor.fast_executemany = True
            cursor.executemany(query, params)
            return True
        except Exception as e:
            msg = str(e).lower()

            if ("1205" in msg or "deadlock" in msg) and attempt <= retries:
                wait = round(0.5 + attempt * 0.5, 2)
                log(f"Deadlock detected. Retry {attempt}/{retries} after {wait}s...")
                time.sleep(wait)
                attempt += 1
                continue

            if ("timeout" in msg or "network" in msg) and attempt <= retries:
                wait = round(0.5 + attempt * 0.5, 2)
                log(f"Transient SQL error. Retry {attempt}/{retries} after {wait}s...")
                time.sleep(wait)
                attempt += 1
                continue

            raise

# ============================================================
# BUILD SQL INSERT QUERY
# ============================================================

def build_insert_query(df: pd.DataFrame, schema: str, table: str):
    cols = list(df.columns)
    col_list = ", ".join([f"[{c}]" for c in cols])
    placeholders = ", ".join(["?"] * len(cols))
    return f"INSERT INTO {schema}.{table} ({col_list}) VALUES ({placeholders})"

# ============================================================
# SQL COLUMN METADATA LOADER
# ============================================================

def load_sql_column_info(schema, table):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}'
    """)

    info = {}
    for col, dtype, length in cursor.fetchall():
        info[col] = (dtype.lower(), length)

    cursor.close()
    conn.close()
    return info

# ============================================================
# MAIN WORKER FUNCTION
# ============================================================

def process_file(file_path, schema, table):
    log(f"Worker started: {os.path.basename(file_path)}")

    conn = get_connection()
    cursor = conn.cursor()

    sql_colinfo = load_sql_column_info(schema, table)

    batch_size = 250_000
    last_duration = None

    dataset = ds.dataset(file_path, format="parquet")
    batches = dataset.to_batches(batch_size=batch_size)

    first_chunk = True
    insert_query = None
    total_rows_inserted = 0

    for batch in batches:
        df = batch.to_pandas(types_mapper=pd.ArrowDtype)

        if first_chunk:
            insert_query = build_insert_query(df, schema, table)
            first_chunk = False

        df = convert_types_for_sql(df, sql_colinfo)
        
        # After convert_types_for_sql()
        for col in df.columns:
            series = df[col]
            if series.dtype == float or "float" in str(series.dtype).lower():
                df[col] = series.replace([np.inf, -np.inf], np.nan)
                df[col] = df[col].where(df[col].notna(), None)

        df = to_python_native(df)

        params = list(df.itertuples(index=False, name=None))

        t0 = time.time()
        safe_insert(cursor, insert_query, params)
        t1 = time.time()

        batch_duration = t1 - t0
        last_duration = batch_duration
        total_rows_inserted += len(df)

        new_batch_size = optimize_batch_size(batch_size, batch_duration)
        if new_batch_size != batch_size:
            batch_size = new_batch_size
            log(f"{os.path.basename(file_path)}: batch size → {batch_size:,}")
            batches = dataset.to_batches(batch_size=batch_size)

        del df, params

    cursor.close()
    conn.close()

    log(f"Finished {os.path.basename(file_path)} — {total_rows_inserted:,} rows")
    return total_rows_inserted

# ============================================================
# PARALLEL ORCHESTRATOR (FINAL SECTION)
# ============================================================

def orchestrate_import(parquet_dir, schema, table):
    start = time.time()

    files = []
    for root, dirs, fns in os.walk(parquet_dir):
        for f in fns:
            if f.lower().endswith(".parquet"):
                files.append(os.path.join(root, f))

    if not files:
        log("No parquet files found.")
        return

    log(f"Discovered {len(files)} files")

    total_est = 0
    for f in files:
        dataset = ds.dataset(f, format="parquet")
        total_est += dataset.count_rows()

    log(f"Estimated total rows: {total_est:,}")

    max_workers = os.cpu_count()
    log(f"Using {max_workers} parallel workers...")

    total_inserted = 0

    with Progress(
        "[progress.description]{task.description}",
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeRemainingColumn(),
        console=console
    ) as progress:

        task = progress.add_task("Importing parquet files...", total=total_est)

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_file, f, schema, table): f
                for f in files
            }

            for future in as_completed(futures):
                f = futures[future]
                try:
                    inserted = future.result()
                    total_inserted += inserted
                    progress.update(task, advance=inserted)
                except Exception as e:
                    log(f"Error processing file {f}: {e}")
                    raise

    end = time.time()
    elapsed = end - start
    minutes = elapsed // 60
    hours = minutes // 60
    minutes = int(minutes % 60)

    log(f"Total rows inserted: {total_inserted:,}")
    log(f"Total duration {int(hours)} hour(s) and {minutes} minute(s).")

# ============================================================
# MAIN EXECUTION
# ============================================================

if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    orchestrate_import(PARQUET_DIR, SCHEMA, TABLE)