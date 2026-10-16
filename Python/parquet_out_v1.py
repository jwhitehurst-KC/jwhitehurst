import gc
import math
import os
import shutil
import time
from datetime import datetime

import pyarrow.parquet as pp
from mssql_python import connect

conn_str ="Server=kcitazrhpasqlprp16.azds.kingcounty.gov;Database=inthealth_edw;Encrypt=yes;TrustServerCertificate=yes;Authentication=ActiveDirectoryIntegrated"
OUTPUT_DIR = "C:/temp/apcd/"

# 2. Establish connection
conn = connect(conn_str)

# Note: bulkcopy_arrow handles its own transaction stream, 
# so autocommit must be True, or the destination table must already exist and be committed.
conn.autocommit = True
cursor = conn.cursor()

nrow = cursor.execute('select count_big(*) as N from stg_claims.stage_apcd_elig_month').fetchall()
nrow = nrow[0][0]
num_rows_per_file = math.ceil(nrow/10)

# Open the connection as an arrow stream
cursor.execute('select * from stg_claims.stage_apcd_elig_month')
reader = cursor.arrow_reader(batch_size = num_rows_per_file)

iter = 0
start_dt = datetime.now()
print(start_dt.strftime("%H:%M:%S") + ": Start SYNAPSE!")
for batch in reader:
    start = time.perf_counter()
    print('Starting iteration ' + str(iter).zfill(3) + ' at ' + datetime.now().strftime("%H:%M:%S"))
    file_name = f"final.apcd_elig_month.{str(iter).zfill(3)}_20261001.parquet"
    out = os.path.join(OUTPUT_DIR, file_name)
    writer = pp.ParquetWriter(out, batch.schema)
    writer.write_batch(batch)
    writer.close()
    del writer
    gc.collect()
    export_dir = "//dphcifs/APDE-CDIP/Mcaid-Mcare/testing/v1_synapse/"
    shutil.move(out, os.path.join(export_dir, file_name))
    end = time.perf_counter()
    print('End iteration ' + str(iter).zfill(3) + ' at ' + datetime.now().strftime("%H:%M:%S") + " | Elapsed time = " + str(round(end-start)/60))  
    iter = iter + 1

end_dt = datetime.now()  
duration = end_dt - start_dt
duration = end_dt - start_dt
total_minutes = int(duration.total_seconds() // 60)
hours, minutes = divmod(total_minutes, 60)
print(end_dt.strftime("%H:%M:%S") + ": Done!")
print(f"Total Duration: {hours} hour(s) and {minutes} minute(s).")

conn_str ="Server=kcitazrhpasqlprp16.azds.kingcounty.gov;Database=hhs_analytics_workspace;Encrypt=yes;TrustServerCertificate=yes;Authentication=ActiveDirectoryIntegrated"

# 2. Establish connection
conn = connect(conn_str)

# Note: bulkcopy_arrow handles its own transaction stream, 
# so autocommit must be True, or the destination table must already exist and be committed.
conn.autocommit = True
cursor = conn.cursor()

nrow = cursor.execute('select count_big(*) as N from claims.final_apcd_elig_month').fetchall()
nrow = nrow[0][0]
num_rows_per_file = math.ceil(nrow/10)

# Open the connection as an arrow stream
cursor.execute('select * from claims.final_apcd_elig_month')
reader = cursor.arrow_reader(batch_size = num_rows_per_file)

iter = 0
start_dt = datetime.now()  
print(start_dt.strftime("%H:%M:%S") + ": Start HHSAW!")
for batch in reader:
    start = time.perf_counter()
    print('Starting iteration ' + str(iter).zfill(3) + ' at ' + datetime.now().strftime("%H:%M:%S"))  
    file_name = f"final.apcd_elig_month.{str(iter).zfill(3)}_20261001.parquet"
    out = os.path.join(OUTPUT_DIR, file_name)
    writer = pp.ParquetWriter(out, batch.schema)
    writer.write_batch(batch)
    writer.close()
    del writer
    gc.collect()
    export_dir = "//dphcifs/APDE-CDIP/Mcaid-Mcare/testing/v1_hhsaw/"
    shutil.move(out, os.path.join(export_dir, file_name))
    end = time.perf_counter()
    print('End iteration ' + str(iter).zfill(3) + ' at ' + datetime.now().strftime("%H:%M:%S") + " | Elapsed time = " + str(round(end-start)/60))  
    iter = iter + 1

end_dt = datetime.now()  
duration = end_dt - start_dt
duration = end_dt - start_dt
total_minutes = int(duration.total_seconds() // 60)
hours, minutes = divmod(total_minutes, 60)
print(end_dt.strftime("%H:%M:%S") + ": Done!")
print(f"Total Duration: {hours} hour(s) and {minutes} minute(s).")


