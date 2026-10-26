import gc
import os
from datetime import datetime

import pandas as pd
import psutil
import pyarrow.parquet as pq
from mssql_python import connect
from tqdm import tqdm

# ============================================================
# Configuration
# ============================================================
SERVER = 'kcitazrhpasqlprp16.azds.kingcounty.gov'
DRIVER = '{ODBC Driver 17 for SQL Server}'

TARGET_FILE_GB = 2
TARGET_FILE_BYTES = int(TARGET_FILE_GB * 1024 * 1024 * 1024)
BATCH_DATE = "20261001"

# Parquet compression options: "snappy", "zstd", "gzip", "brotli", "lz4"
PARQUET_COMPRESSION = "zstd"          # Recommended for SQL workloads
USE_DICTIONARY_ENCODING = True        # Better compression for string/categorical data


# ============================================================
# Adaptive batch sizing based on available memory
# ============================================================
def calculate_batch_size():
    """
    Estimate Arrow batch size (rows per batch) based on available memory.
    Uses ~10% of available RAM and assumes ~800 bytes per row.
    """
    mem = psutil.virtual_memory()
    avail = mem.available

    EST_ROW_SIZE = 800  # bytes/row median size estimate
    safe_bytes = int(avail * 0.10)

    est_rows = safe_bytes // EST_ROW_SIZE

    # Constrain within safe limits
    return max(50_000, min(est_rows, 2_000_000))


# ============================================================
# Start
# ============================================================
start_dt = datetime.now()
print(start_dt.strftime("%H:%M:%S") + ": Start Export!")


# ============================================================
# Input table list
# ============================================================
data = {
    "db_name": ["hhs_analytics_workspace", "inthealth_edw"],
    "schema_name": ["claims", "stg_claims"],
    "table_name": ["final_apcd_elig_month", "stage_apcd_elig_month"],
    "export_dir": [
        "//dphcifs/APDE-CDIP/Mcaid-Mcare/testing/v2_hhsaw/",
        "//dphcifs/APDE-CDIP/Mcaid-Mcare/testing/v2_synapse/"
    ]
}
df = pd.DataFrame(data)


# ============================================================
# Main export loop
# ============================================================
for _, row in df.iterrows():

    # Build connection string
    conn_str = f"""
        Server={SERVER};
        Database={row['db_name']};
        Encrypt=yes;
        TrustServerCertificate=yes;
        Authentication=ActiveDirectoryIntegrated
    """

    conn = connect(conn_str)
    conn.autocommit = True
    cursor = conn.cursor()

    # Count rows for progress bar
    nrow = cursor.execute(
        f"SELECT COUNT_BIG(*) FROM {row['schema_name']}.{row['table_name']}"
    ).fetchone()[0]

    print(f"\nExporting {row['schema_name']}.{row['table_name']} ({nrow:,} rows)")

    # Adaptive batch size
    ARROW_BATCH_ROWS = calculate_batch_size()
    print(f"Adaptive batch size: {ARROW_BATCH_ROWS:,} rows per batch")

    # Run SELECT
    cursor.execute(f"SELECT * FROM {row['schema_name']}.{row['table_name']}")
    reader = cursor.arrow_reader(batch_size=ARROW_BATCH_ROWS)

    # Ensure directory exists
    os.makedirs(row['export_dir'], exist_ok=True)

    # Track file writing state
    file_index = 1
    current_writer = None
    current_file_path = None
    bytes_written = 0

    # Progress bar
    pbar = tqdm(total=nrow, unit="rows", desc=row["table_name"])

    def start_new_file(file_index):
        file_name = f"{row['table_name']}.{str(file_index).zfill(3)}_{BATCH_DATE}.parquet"
        file_path = os.path.join(row['export_dir'], file_name)
        print(f"\nStarting new file: {file_name}")
        return file_path, 0  # reset bytes_written

    # Start first file
    current_file_path, bytes_written = start_new_file(file_index)
    file_index += 1

    # Stream batches
    for batch in reader:

        # Writer lazy init on first batch
        if current_writer is None:
            current_writer = pq.ParquetWriter(
                current_file_path,
                batch.schema,
                compression=PARQUET_COMPRESSION,
                use_dictionary=USE_DICTIONARY_ENCODING
            )

        current_writer.write_batch(batch)
        pbar.update(batch.num_rows)
        
        # Check actual file size on disk
        actual_size = os.path.getsize(current_file_path)

        if actual_size >= TARGET_FILE_BYTES:
            print(f"Rolling file at {actual_size/1024/1024/1024:.2f} GB")
            current_writer.close()
            current_writer = None
            gc.collect()

            current_file_path, bytes_written = start_new_file(file_index)
            file_index += 1

    # Close last file
    if current_writer:
        current_writer.close()

    pbar.close()
    print(f"Finished exporting {row['schema_name']}.{row['table_name']}")


# ============================================================
# Finish
# ============================================================
end_dt = datetime.now()
duration = end_dt - start_dt
total_minutes = int(duration.total_seconds() // 60)
hours, minutes = divmod(total_minutes, 60)

print("\n" + end_dt.strftime("%H:%M:%S") + ": Done!")
print(f"Total Duration: {hours} hour(s) and {minutes} minute(s).")