import os
import urllib
from datetime import datetime

start_dt = datetime.now()
print(f"{start_dt.strftime("%H:%M:%S")}: Writing initial chunk")

end_dt = datetime.now()
print(f"{end_dt.strftime("%H:%M:%S")}: Complete")

duration = end_dt - start_dt
total_minutes = int(duration.total_seconds() // 60)
hours, minutes = divmod(total_minutes, 60)
print(f"Total Duration: {hours} hour(s) and {minutes} minute(s).")
