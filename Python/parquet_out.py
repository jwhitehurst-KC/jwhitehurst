import gc
import math
import os
import urllib
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
from mssql_python import connect
from sqlalchemy import create_engine, text

# 1. Configuration (Works for both Azure SQL and Azure Synapse Dedicated SQL Pools)
SERVER = 'kcitazrhpasqlprp16.azds.kingcounty.gov'
DRIVER = '{ODBC Driver 17 for SQL Server}' # Ensure this driver is installed on your OS

# Configuration Boundaries
TARGET_FILE_SIZE = 1.9 * 1024 * 1024 * 1024  # Safely target ~1.9 GB in bytes
ROW_BATCH_SIZE = 150000                     # Number of rows to pull into memory at once

def stream_azure_to_parquet_v1(export_dir, db_name, schema_name, table_name):
    # 2. Build the Connection String
    conn_str = f"Server={SERVER};Database={db_name};Encrypt=yes;TrustServerCertificate=yes;Authentication=ActiveDirectoryIntegrated"
    conn = connect(conn_str)

    # Note: bulkcopy_arrow handles its own transaction stream, 
    # so autocommit must be True, or the destination table must already exist and be committed.
    conn.autocommit = True
    cursor = conn.cursor()

    nrow = cursor.execute(f"select count_big(*) as N from {schema_name}.{table_name}").fetchall()
    nrow = nrow[0][0]
    num_rows_per_file = math.ceil(nrow/10)

    # Open the connection as an arrow stream
    cursor.execute(f"select * from {schema_name}.{table_name}")
    reader = cursor.arrow_reader(batch_size = num_rows_per_file)
    os.makedirs(export_dir, exist_ok=True)

    iter = 0
    start_dt = datetime.now()  # noqa: DTZ005
    print(start_dt.strftime("%H:%M:%S") + ": Start SYNAPSE!")
    for batch in reader:
        print('Starting iteration ' + str(iter).zfill(3) + ' at ' + datetime.now().strftime("%H:%M:%S"))  # noqa: DTZ005
        file_name = f"final.apcd_elig_month.{str(iter).zfill(3)}_20261001.parquet"
        out = os.path.join(export_dir, file_name)
        writer = pq.ParquetWriter(out, batch.schema)
        writer.write_batch(batch)
        writer.close()
        del writer
        gc.collect()
        export_dir = "//dphcifs/APDE-CDIP/Mcaid-Mcare/testing/v1_synapse/"
        print('End iteration ' + str(iter).zfill(3) + ' at ' + datetime.now().strftime("%H:%M:%S"))  # noqa: DTZ005
        iter = iter + 1

    end_dt = datetime.now()  # noqa: DTZ005
    duration = end_dt - start_dt
    duration = end_dt - start_dt
    total_minutes = int(duration.total_seconds() // 60)
    hours, minutes = divmod(total_minutes, 60)
    print(end_dt.strftime("%H:%M:%S") + ": Done!")
    print(f"Total Duration: {hours} hour(s) and {minutes} minute(s).")

def stream_azure_to_parquet_v2(export_dir, db_name, schema_name, table_name):
    # 2. Build the Connection String
    params = urllib.parse.quote_plus(
        f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={db_name};"
        f"Encrypt=yes;TrustServerCertificate=yes;"
        f"Authentication=ActiveDirectoryIntegrated;"
        f"autocommit=True;")

    connection_uri = f"mssql+pyodbc:///?odbc_connect={params}"

    # Use 'fast_executemany' for general pyodbc performance optimizations
    engine = create_engine(connection_uri, fast_executemany=True)#.execution_options(isolation_level="AUTOCOMMIT")
    os.makedirs(export_dir, exist_ok=True)
    
    file_counter = 1
    current_file_name = f"final.apcd_elig_month.{str(file_counter).zfill(3)}_20261001.parquet"
    current_file_path = os.path.join(export_dir, current_file_name)
    
    # Establish server-side cursor streaming
    query = f"select * from {schema_name}.{table_name}"
    with engine.connect() as conn:
        # 'stream_results=True' tells SQLAlchemy/pyodbc to fetch row blocks on-demand
        result_proxy = conn.execution_options(stream_results=True).execute(text(query))
        
        # Pull the first batch to dynamically map the database schema to Arrow
        first_chunk = result_proxy.fetchmany(ROW_BATCH_SIZE)
        if not first_chunk:
            print("No data retrieved from the query.")
            return
            
        # 1. Convert SQLAlchemy Row objects into standard python dictionaries
        # using the '.mappings()' layer or a list comprehension
        chunk_dicts = [dict(row._mapping) for row in first_chunk]
        
        # 2. Correct PyArrow method to create a table from a list of dicts
        inferred_table = pa.Table.from_pylist(chunk_dicts)
        inferred_schema = inferred_table.schema
        
        # 3. Create a clean, upgraded schema by catching and swapping types
        upcasted_fields = []
        for field in inferred_schema:
            # A. Catch TINYINT / SMALLINT and map to INT32
            if pa.types.is_int8(field.type) or pa.types.is_int16(field.type):
                upcasted_fields.append(pa.field(field.name, pa.int32(), nullable=field.nullable))
            # B. Catch Timestamps and force Millisecond precision WITHOUT Timezones
            elif pa.types.is_timestamp(field.type):
                # Synapse requires timestamp('ms') and will throw errors if tz is present
                upcasted_fields.append(pa.field(field.name, pa.timestamp('ms', tz=None), nullable=field.nullable))
            # C. Catch Dates (no time element) and keep standard date32
            elif pa.types.is_date(field.type):
                upcasted_fields.append(pa.field(field.name, pa.date32(), nullable=field.nullable))        
            else:
                # Keep all other types (string, bigint, decimals) intact
                upcasted_fields.append(field)

        # Build the final, safe target schema
        schema = pa.schema(upcasted_fields)
        table = pa.Table.from_pylist(chunk_dicts, schema=schema)

        # Initialize the file writer
        
        start_dt = datetime.now()  # noqa: DTZ005
        print(f"{datetime.now().strftime("%H:%M:%S")}: Writing initial chunk to: {current_file_path}")  # noqa: DTZ005
        writer = pq.ParquetWriter(
            current_file_path, 
            schema, 
            compression='snappy',
            version="2.6", 
            use_deprecated_int96_timestamps=False)
        writer.write_table(table)
        
        while True:
            chunk = result_proxy.fetchmany(ROW_BATCH_SIZE)
            if not chunk:
                break # Database streaming completely finished
                
            # Convert this chunk's SQLAlchemy rows to a list of dicts
            chunk_dicts = [dict(row._mapping) for row in chunk]
            
            # Enforce the established schema when building the next Arrow Table chunk
            table = pa.Table.from_pylist(chunk_dicts, schema=schema)
            writer.write_table(table)
            
            # Check the actual compressed size of the file on disk
            if os.path.getsize(current_file_path) >= TARGET_FILE_SIZE:
                writer.close() 
                del writer
                gc.collect()
                print(f"{datetime.now().strftime("%H:%M:%S")}: File {current_file_path} reached target limit (~2GB). Closing file.")  # noqa: DTZ005
                
                # Move completed file, Rotate and initialize the next physical segment
                file_counter += 1
                current_file_name = f"final.apcd_elig_month.{str(file_counter).zfill(3)}_20261001.parquet"
                current_file_path = os.path.join(export_dir, current_file_name)
                writer = pq.ParquetWriter(
                    current_file_path, 
                    schema, 
                    compression='snappy',
                    version="2.6", 
                    use_deprecated_int96_timestamps=False)
                print(f"{datetime.now().strftime("%H:%M:%S")}: Created new file segment: {current_file_path}")  # noqa: DTZ005
                
        writer.close()
        del writer
        gc.collect()
        
        end_dt = datetime.now()  # noqa: DTZ005
        duration = end_dt - start_dt
        duration = end_dt - start_dt
        total_minutes = int(duration.total_seconds() // 60)
        hours, minutes = divmod(total_minutes, 60)

        print(end_dt.strftime("%H:%M:%S") + ": Successfully exported Azure data into sized Parquet chunks!")
        print(f"Total Duration: {hours} hour(s) and {minutes} minute(s).")



#stream_azure_to_parquet_v2(
#    export_dir = "//dphcifs/APDE-CDIP/Mcaid-Mcare/testing/v2_hhsaw/", 
#    db_name = "hhs_analytics_workspace", 
#    schema_name = "claims", 
#    table_name = "final_apcd_elig_month")
#stream_azure_to_parquet_v2(
#    export_dir = "//dphcifs/APDE-CDIP/Mcaid-Mcare/testing/v2_synapse/", 
#    db_name = "inthealth_edw", 
#    schema_name = "stg_claims", 
#    table_name = "stage_apcd_elig_month")

stream_azure_to_parquet_v1(
    export_dir = "//dphcifs/APDE-CDIP/Mcaid-Mcare/testing/v1_hhsaw/", 
    db_name = "hhs_analytics_workspace", 
    schema_name = "claims", 
    table_name = "final_apcd_elig_month")
stream_azure_to_parquet_v1(
    export_dir = "//dphcifs/APDE-CDIP/Mcaid-Mcare/testing/v1_synapse/", 
    db_name = "inthealth_edw", 
    schema_name = "stg_claims", 
    table_name = "stage_apcd_elig_month")



writer = pq.ParquetWriter(
    "//dphcifs/APDE-CDIP/Mcaid-Mcare/testing/v2_hhsaw/final.apcd_elig_month.002_20261002.parquet", 
    compression='snappy',
    version="2.6", 
    use_deprecated_int96_timestamps=False)
writer.close()
del writer
gc.collect()