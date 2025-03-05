## working code with incremental load and replace table creation
import dlt
import requests
import pandas as pd
from datetime import date, datetime

# API Key
key = 'f54c03642bed081bc945c8e9'

# API Call
url = f'https://v6.exchangerate-api.com/v6/f54c03642bed081bc945c8e9/latest/USD'
response = requests.get(url)
data = response.json()

# Normalize API response into DataFrame
df = pd.json_normalize(data['conversion_rates'])
df = df.melt().reset_index()
df["index"] += 1
df['date'] = date.today()
df['timestamp'] = datetime.utcnow()  # Add timestamp for incremental tracking
df = df.rename(columns={'index': 'id', 'variable': 'currencycode', 'value': 'fxrate'})

# Convert DataFrame to list of records
records = df.to_dict(orient="records")

# Initialize pipeline
pipeline = dlt.pipeline(
    pipeline_name="fxrate_pipeline",
    destination="postgres",
    dataset_name="incremental"
)

# Define incremental resource
@dlt.resource(
    write_disposition="replace",
    primary_key=("currencycode", "date"),
    incremental=dlt.sources.incremental("timestamp")
)
def fxrate_data():
    yield records

# Run the pipeline
load_info = pipeline.run(fxrate_data())

print(load_info)
