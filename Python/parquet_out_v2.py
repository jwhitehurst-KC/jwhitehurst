import os
import urllib
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text

# 1. Configuration (Works for both Azure SQL and Azure Synapse Dedicated SQL Pools)
SERVER = 'kcitazrhpasqlprp16.azds.kingcounty.gov'
DATABASE = 'hhs_analytics_workspace'
DRIVER = '{ODBC Driver 17 for SQL Server}' # Ensure this driver is installed on your OS

QUERY = "select * from claims.final_apcd_elig_month"
#OUTPUT_DIR = "//dphcifs/APDE-CDIP/SFTP_APDEDATA/APDEDataExchange/WA-APCD/export/"
OUTPUT_DIR = "C:/temp/apcd/final_schema/"

# Configuration Boundaries
TARGET_FILE_SIZE = 1.9 * 1024 * 1024 * 1024  # Safely target ~1.9 GB in bytes
ROW_BATCH_SIZE = 150000                     # Number of rows to pull into memory at once

# 2. Build the Connection String
params = urllib.parse.quote_plus(
    f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};"
    f"Encrypt=yes;TrustServerCertificate=yes;"
    f"Authentication=ActiveDirectoryInteractive;"
    #f"autocommit=True;"
)
connection_uri = f"mssql+pyodbc:///?odbc_connect={params}"

# Use 'fast_executemany' for general pyodbc performance optimizations
engine = create_engine(connection_uri, fast_executemany=True)#.execution_options(isolation_level="AUTOCOMMIT")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def stream_azure_to_parquet():
    file_counter = 1
    current_file_path = os.path.join(OUTPUT_DIR, f"final.apcd_elig_month.{str(file_counter).zfill(3)}_20261001.parquet")
    
    # Establish server-side cursor streaming
    with engine.connect() as conn:
        # 'stream_results=True' tells SQLAlchemy/pyodbc to fetch row blocks on-demand
        result_proxy = conn.execution_options(stream_results=True).execute(text(QUERY))
        
        # Pull the first batch to dynamically map the database schema to Arrow
        first_chunk = result_proxy.fetchmany(ROW_BATCH_SIZE)
        if not first_chunk:
            print("No data retrieved from the query.")
            return
            
        # 1. Convert SQLAlchemy Row objects into standard python dictionaries
        # using the '.mappings()' layer or a list comprehension
        chunk_dicts = [dict(row._mapping) for row in first_chunk]
        
        # 2. Correct PyArrow method to create a table from a list of dicts
        table = pa.Table.from_pylist(chunk_dicts)
        schema = table.schema
        
        # Initialize the file writer
        
        print(f"{datetime.now().strftime("%H:%M:%S")}: Writing initial chunk to: {current_file_path}")
        writer = pq.ParquetWriter(current_file_path, schema, compression='snappy')
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
                print(f"{datetime.now().strftime("%H:%M:%S")}: File {current_file_path} reached target limit (~2GB). Closing file.")
                
                # Rotate and initialize the next physical segment
                file_counter += 1
                current_file_path = os.path.join(OUTPUT_DIR, f"final.apcd_elig_month.{str(file_counter).zfill(3)}_20261001.parquet")
                writer = pq.ParquetWriter(current_file_path, schema, compression='snappy')
                print(f"{datetime.now().strftime("%H:%M:%S")}: Created new file segment: {current_file_path}")
                
        writer.close()
        print(datetime.now().strftime("%H:%M:%S") + ": Successfully exported Azure data into sized Parquet chunks!")

if __name__ == "__main__":
    stream_azure_to_parquet()
