import os
import pandas as pd

RAW_PATH = "/opt/airflow/data/raw/online_retail_II.csv"
PROCESSED_PATH = "/opt/airflow/data/processed/cleaned_retail.csv"

def transform_data():
    print("=== TRANSFORM STAGE STARTED ===")

    if not os.path.exists(RAW_PATH):
        raise FileNotFoundError("Raw file missing. Cannot transform.")

    # Load data
    df = pd.read_csv(RAW_PATH)
    print(f"Original shape: {df.shape}")

    # Drop null important columns
    df = df.dropna(subset=["Invoice", "Customer ID"])

    # Convert data types safely
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")

    # Remove invalid rows
    df = df[(df["Quantity"] > 0) & (df["Price"] > 0)]

    # Handle missing description
    df["Description"] = df["Description"].fillna("Unknown Product")

    # Feature engineering
    df["TotalAmount"] = df["Quantity"] * df["Price"]

    print(f"Cleaned shape: {df.shape}")

    # Ensure processed directory exists
    os.makedirs(os.path.dirname(PROCESSED_PATH), exist_ok=True)

    # Save file
    df.to_csv(PROCESSED_PATH, index=False)

    print(f"File saved at {PROCESSED_PATH}")
    print("=== TRANSFORM STAGE COMPLETED ===")