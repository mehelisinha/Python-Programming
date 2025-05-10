#importing required libraries
import dlt
import pandas as pd
from datetime import datetime

# Load the dataset
file_path = "bank_transaction.csv"
df = pd.read_csv(file_path)

# Data Cleaning
# Standardize column names
df.columns = df.columns.str.replace(" ", "")

# Convert data types
df["TransactionID"] = df["TransactionID"].astype(str)
df["AccountID"] = df["AccountID"].astype(str)
df["DeviceID"] = df["DeviceID"].astype(str)
df["MerchantID"] = df["MerchantID"].astype(str)
df["IPAddress"] = df["IPAddress"].astype(str)
df["TransactionAmount"] = df["TransactionAmount"].astype(float)
df["AccountBalance"] = df["AccountBalance"].astype(float)
df["CustomerAge"] = df["CustomerAge"].astype(int)
df["TransactionDuration"] = df["TransactionDuration"].astype(int)
df["LoginAttempts"] = df["LoginAttempts"].astype(int)
df["TransactionDate"] = pd.to_datetime(df["TransactionDate"], errors='coerce')
df["PreviousTransactionDate"] = pd.to_datetime(df["PreviousTransactionDate"], errors='coerce')

# Drop rows with missing values
df = df.dropna()

# Convert data to dictionary format for DLT
records = df.to_dict(orient="records")

# Define the DLT pipeline
pipeline = dlt.pipeline(
    pipeline_name="bank_transactions_pipeline",
    destination="postgres",
    dataset_name="incremental"
)

# Declare DLT resource
@dlt.resource(
        table_name="transactions",
        primary_key="TransactionID", 
        write_disposition="merge"
        )
def bank_transactions():
    yield records

# Run the pipeline
load_info = pipeline.run(bank_transactions())

print(load_info)
