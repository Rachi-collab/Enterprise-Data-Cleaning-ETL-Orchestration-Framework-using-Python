import os

PROCESSED_PATH = "/opt/airflow/data/processed/cleaned_retail.csv"

def load_data():
    print("=== LOAD STAGE STARTED ===")

    if not os.path.exists(PROCESSED_PATH):
        raise FileNotFoundError("Processed file not found. Transformation may have failed.")

    print("Processed file verified successfully.")
    print("=== LOAD STAGE COMPLETED ===")