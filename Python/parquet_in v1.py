import datetime

import pyarrow.parquet as pp
from mssql_python import connect

start_dt = datetime.now()
print("Start Import!..." + start_dt.strftime("%H:%M:%S"))

SERVER = 'kcitazrhpasqlprp16.azds.kingcounty.gov'
DRIVER = '{ODBC Driver 17 for SQL Server}'
DATABASE = 'hhs_analytics_workspace'

SQL_CONNECTION_STRING = f"""
        Server={SERVER};
        Database={DATABASE};
        Encrypt=yes;
        TrustServerCertificate=yes;
        Authentication=ActiveDirectoryIntegrated
    """

conn = connect(SQL_CONNECTION_STRING)

cursor = conn.cursor()

print("Reading files..." + datetime.now().strftime("%H:%M:%S"))
file_1 = pp.ParquetFile('//dphcifs/APDE-CDIP/Mcaid-Mcare/testing/v2_hhsaw/final_apcd_elig_month.001_20261001.parquet')
file_2 = pp.ParquetFile('//dphcifs/APDE-CDIP/Mcaid-Mcare/testing/v2_hhsaw/final_apcd_elig_month.002_20261001.parquet')

# Function from google
def parquet_to_mssql_create_table(parquet_path: str, table_name: str) -> str:
    """Reads a Parquet file schema and generates a SQL Server CREATE TABLE statement."""
    print("Creating Table..." + datetime.now().strftime("%H:%M:%S"))
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



tn = '[jwhitehurst].[parquet_test_mssql]'
drop = 'drop table if exists ' + tn
create_tab = parquet_to_mssql_create_table(file_1, tn)
cursor.execute(drop).execute(create_tab).commit()

# Upload 
print("Begin loading file 1..." + datetime.now().strftime("%H:%M:%S"))
cursor.bulkcopy_arrow(tn, file_1.iter_batches(), timeout=60*60*3)
print("Loading complete..." + datetime.now().strftime("%H:%M:%S"))
print("Begin loading file 2..." + datetime.now().strftime("%H:%M:%S"))
cursor.bulkcopy_arrow(tn, file_2.iter_batches(), timeout=60*60*3)
print("Loading complete..." + datetime.now().strftime("%H:%M:%S"))
end_dt = datetime.now()
duration = end_dt - start_dt
total_minutes = int(duration.total_seconds() // 60)
hours, minutes = divmod(total_minutes, 60)

print("\n" + end_dt.strftime("%H:%M:%S") + ": Done!")
print(f"Total Duration: {hours} hour(s) and {minutes} minute(s).")