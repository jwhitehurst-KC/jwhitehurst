from mssql_python import connect
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pp
import time

SQL_CONNECTION_STRING="Server=kcitazrhpasqlprp16.azds.kingcounty.gov;Database=hhs_analytics_workspace;Encrypt=yes;TrustServerCertificate=yes;Authentication=ActiveDirectoryInteractive"

conn = connect(SQL_CONNECTION_STRING)

cursor = conn.cursor()

l_fp = 'C:/Users/DCASEY.KC/local_documents/sql_up_test.parquet'
w_fp = 'C:/Users/DCASEY.KC/local_documents/sql_up_test_wide.parquet'
long = pp.ParquetFile(l_fp)
wide = pp.ParquetFile(w_fp)

# Function from google
def parquet_to_mssql_create_table(parquet_path: str, table_name: str) -> str:
    """Reads a Parquet file schema and generates a SQL Server CREATE TABLE statement."""
    # 1. Read metadata/schema without loading the actual data rows
    schema = pp.read_schema(parquet_path)
    
    # 2. Map PyArrow types to MSSQL types
    # Customize lengths or mappings based on your typical data profile
    type_mapping = {
        "bool": "BIT",
        "int8": "TINYINT",
        "int16": "SMALLINT",
        "int32": "INT",
        "int64": "BIGINT",
        "uint8": "TINYINT",
        "uint16": "SMALLINT",
        "uint32": "INT",
        "uint64": "BIGINT",
        "float": "REAL",
        "double": "FLOAT",
        "string": "VARCHAR(100)",
        "binary": "VARBINARY(MAX)",
        "date32[day]": "DATE",
        "timestamp[s]": "DATETIME2",
        "timestamp[ms]": "DATETIME2",
        "timestamp[us]": "DATETIME2",
        "timestamp[ns]": "DATETIME2",
    }
    
    columns_ddl = []
    
    for field in schema:
        field_name = field.name
        arrow_type = str(field.type)
        
        # Handle specialized types like Decimal dynamically
        if arrow_type.startswith("decimal"):
            # e.g., decimal128(18, 2) -> DECIMAL(18, 2)
            mssql_type = arrow_type.replace("decimal128", "DECIMAL").replace("decimal256", "DECIMAL")
        else:
            # Fall back to NVARCHAR(MAX) if type isn't explicitly mapped
            mssql_type = type_mapping.get(arrow_type, "NVARCHAR(MAX)")
            
        # Wrap column names in brackets to handle spaces or reserved keywords safely
        columns_ddl.append(f"    [{field_name}] {mssql_type}")
        
    # 3. Assemble the final T-SQL command
    ddl_body = ",\n".join(columns_ddl)
    create_table_command = f"CREATE TABLE {table_name} (\n{ddl_body}\n);"
    
    return create_table_command

tabs = ['long_1', 'long_2', 'wide_1', 'wide_2']

# create the columns
for t in tabs:
    if t in ['long_1', 'long_2']:
        fp = l_fp
    else:
        fp = w_fp
    
    tn = '[dcasey].[test_py_up_' + t +']'

    drop = 'drop table if exists ' + tn
    create_tab = parquet_to_mssql_create_table(fp, tn)
    cursor.execute(drop).execute(create_tab).commit()

# Upload long
s1 = time.perf_counter()
cursor.bulkcopy_arrow('[dcasey].[test_py_up_' + 'long_1' +']', long.iter_batches(), timeout=60*60*3)
e1 = time.perf_counter()

# Upload wide
s2 = time.perf_counter()
cursor.bulkcopy_arrow('[dcasey].[test_py_up_' + 'wide_1' +']', wide.iter_batches(), timeout=60*60*3)
e2 = time.perf_counter()

(e1 - s1) / 60
(e2 - s2) / 60

# Long as arrow
s3 = time.perf_counter()
# this I think 
long_dl = cursor.execute("select * from " + '[dcasey].[test_py_up_' + 'long_1' +']').arrow_reader()
cursor.bulkcopy_arrow('[dcasey].[test_py_up_' + 'long_2' +']', long_dl, timeout=60*60*3)
e3 = time.perf_counter()

# Long as arrow
s4 = time.perf_counter()
wide_dl = cursor.execute("select * from " + '[dcasey].[test_py_up_' + 'wide_1' +']').arrow_reader()
cursor.bulkcopy_arrow('[dcasey].[test_py_up_' + 'wide_2' +']', wide_dl, timeout=60*60*3)
e4 = time.perf_counter()

(e3 - s3) / 60
(e4 - s4) / 60
