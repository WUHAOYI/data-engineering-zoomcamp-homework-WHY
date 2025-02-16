import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import duckdb
import pandas as pd

client = RESTClient(
    base_url="https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
)

@dlt.resource
def load_ny_taxi_data():
    url = "/ny_taxi_data"
    paginator = PageNumberPaginator(client)

    page_number = 1
    page_size = 1000

    while True:
        response = client.get(url, params={"page": page_number, "page_size": page_size})

        data = response.json()
        if not data:
            break

        yield data
        page_number += 1


pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data"
)

load_info = pipeline.run(load_ny_taxi_data)
# print(load_info)

conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")
df = conn.sql("DESCRIBE").df()
print(df)

# show how many tables
tables = conn.sql("SHOW TABLES").df()
print(tables)

# show the total number of records extracted
df = pipeline.dataset(dataset_type="default").ny_taxi.df()
total_records = len(df)
print(f"Total records: {total_records}")

# Prints column values of the first row
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM rides;
            """
        )
    print(res)