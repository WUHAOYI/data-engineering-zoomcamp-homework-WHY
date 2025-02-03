import gzip
import requests
import pandas as pd
from kestra import Kestra

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{}.csv.gz"

total_lines = 0

for month in range(1, 13):
    month_str = f"{month:02d}"
    file_url = BASE_URL.format(month_str)
    local_file = f"green_tripdata_2020-{month_str}.csv.gz"

    print(f"Downloading {local_file} ...")
    response = requests.get(file_url, stream=True)

    if response.status_code == 200:
        with open(local_file, "wb") as f:
            f.write(response.content)

        with gzip.open(local_file, "rt") as f:
            df = pd.read_csv(f)
            line_count = len(df)
            total_lines += line_count

        print(f"{local_file} has {line_count} rows.")
    else:
        print(f"Failed to download {local_file}")

Kestra.outputs({"total_lines": total_lines})