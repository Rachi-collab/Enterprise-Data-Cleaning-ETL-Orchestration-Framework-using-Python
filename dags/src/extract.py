import os

RAW_PATH = "/opt/airflow/data/raw/online_retail_II.csv"

def extract_data():
    print("=== EXTRACT STAGE STARTED ===")

    if not os.path.exists(RAW_PATH):
        raise FileNotFoundError(f"Raw dataset not found at {RAW_PATH}")

    print("Raw dataset found successfully.")
    print("=== EXTRACT STAGE COMPLETED ===")