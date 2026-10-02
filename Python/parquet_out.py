from mssql_python import connect
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pp
import time
import keyring
import math
from datetime import datetime

SQL_CONNECTION_STRING="Server=kcitazrhpasqlprp16.azds.kingcounty.gov;Database=inthealth_edw;Encrypt=yes;TrustServerCertificate=yes;Authentication=ActiveDirectoryInteractive"

conn = connect(SQL_CONNECTION_STRING)
cursor = conn.cursor()

# source: stg_claims.apcd_claim_pharmacy
## How many rows?
nrow = cursor.execute('select count(*) as N from stg_claims.apcd_pharmacy_claim').fetchall()
nrow = nrow[0][0]
num_rows_per_file = math.ceil(nrow/150)

# Open the connection as an arrow stream
cursor.execute('select * from stg_claims.apcd_pharmacy_claim')
reader = cursor.arrow_reader(batch_size = num_rows_per_file)

iter = 0
print(datetime.now().strftime("%H:%M:%S"))
for batch in reader:
    start = time.perf_counter()
    print('Starting iteration ' + str(iter) + ' at ' + datetime.now().strftime("%H:%M:%S"))
    out = "//dphcifs/APDE-CDIP/SFTP_APDEDATA/APDEDataExchange/WA-APCD/export/output" + str(iter) + ".parquet"
    writer = pp.ParquetWriter(out, batch.schema)
    writer.write_batch(batch)
    writer.close()
    end = time.perf_counter()
    print('End iteration ' + str(iter) + ' at ' + datetime.now().strftime("%H:%M:%S") + " | Elapsed time = " + str(round(end-start)/60))
    iter = iter + 1

print('Done!')
