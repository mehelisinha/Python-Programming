import dlt
import requests
from datetime import datetime

# Define a DLT source
@dlt.source
def weather_source():
    return get_weather_data

# Define a DLT resource with incremental loading
@dlt.resource(write_disposition="replace", primary_key="timestamp", incremental=dlt.sources.incremental("timestamp"))
def get_weather_data():
    """Fetches weather data from Open-Meteo API"""
    
    # Define the API URL (Berlin example, modify as needed)
    API_URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m"

    # Fetch data from the API
    response = requests.get(API_URL)
    data = response.json()

    # Extract the required fields
    timestamps = data["hourly"]["time"]
    temperatures = data["hourly"]["temperature_2m"]

    # Yield records as dictionaries
    for i in range(len(timestamps)):
        yield {
            "timestamp": timestamps[i],
            "temperature": temperatures[i],
            "retrieved_at": datetime.utcnow().isoformat()  # Track data retrieval time
        }

# Load data into PostgreSQL
pipeline = dlt.pipeline(
    pipeline_name="weather_pipeline",
    destination="postgres",
    dataset_name="incremental",
    #credentials="secrets.toml"  # Use credentials from secrets.toml
)

# Run the pipeline
load_info = pipeline.run(weather_source())

print(load_info)
