import mssql_python  # Ensure you run: pip install mssql-python pyarrow
import pyarrow.parquet as pq

# 1. Standard on-premises connection string
# Change port 1433 if your instance uses a custom port or a named instance.
CONN_STR = (
    "Server=YOUR_SERVER_NAME,1433;"
    "Database=YourDatabaseName;"
    "UID=your_sql_username;"
    "PWD=your_sql_password;"
    "Encrypt=yes;"                  # Highly recommended even on-premises
    "TrustServerCertificate=yes;"   # Skips strict chain validation for self-signed on-prem certs
)

# 2. Establish connection
conn = mssql_python.connect(CONN_STR)

# Note: bulkcopy_arrow handles its own transaction stream, 
# so autocommit must be True, or the destination table must already exist and be committed.
conn.autocommit = True
cursor = conn.cursor()

# 3. Stream the parquet file using a RecordBatchReader
# Using 'iter_batches' ensures you don't even pull entire row groups into RAM.
parquet_file = pq.ParquetFile("large_on_prem_data.parquet")

# Process in chunks of 50,000 rows at a time
batch_reader = parquet_file.iter_batches(batch_size=50000)

print("Streaming Parquet data into on-premises SQL Server...")

for batch in batch_reader:
    # Streams the raw Arrow buffer directly into TDS bulk load packets in the native C++ layer
    cursor.bulkcopy_arrow(
        table_name="dbo.TargetTable",
        arrow_data=batch
    )

print("Upload complete!")
cursor.close()
conn.close()
