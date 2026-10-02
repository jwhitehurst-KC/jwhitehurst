from mssql_python import connect
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pp
import time
import keyring
import math
from datetime import datetime

CONN_STR ="Server=kcitazrhpasqlprp16.azds.kingcounty.gov;Database=inthealth_edw;Encrypt=yes;TrustServerCertificate=yes;Authentication=ActiveDirectoryInteractive"

# 2. Establish connection
conn = connect(CONN_STR)

# Note: bulkcopy_arrow handles its own transaction stream, 
# so autocommit must be True, or the destination table must already exist and be committed.
conn.autocommit = True
cursor = conn.cursor()

nrow = cursor.execute('select count_big(*) as N from stg_claims.apcd_eligibility').fetchall()
nrow = nrow[0][0]
num_rows_per_file = math.ceil(nrow/50)

# Open the connection as an arrow stream
cursor.execute('select * from stg_claims.apcd_eligibility')
reader = cursor.arrow_reader(batch_size = num_rows_per_file)

iter = 0
print(datetime.now().strftime("%H:%M:%S"))
for batch in reader:
    start = time.perf_counter()
    print('Starting iteration ' + str(iter).zfill(3) + ' at ' + datetime.now().strftime("%H:%M:%S"))
    out = "//dphcifs/APDE-CDIP/SFTP_APDEDATA/APDEDataExchange/WA-APCD/export/stage.apcd_eligibility." + str(iter).zfill(3) + "_20261001.parquet"
    writer = pp.ParquetWriter(out, batch.schema)
    writer.write_batch(batch)
    writer.close()
    end = time.perf_counter()
    print('End iteration ' + str(iter).zfill(3) + ' at ' + datetime.now().strftime("%H:%M:%S") + " | Elapsed time = " + str(round(end-start)/60))
    iter = iter + 1

print('Done!')








