import os
import pandas as pd
import logging

RAW_PATH = "/opt/airflow/data/raw/online_retail_II.csv"
PROCESSED_PATH = "/opt/airflow/data/processed/cleaned_retail.csv"

def transform_data():
    logging.info("Transform stage started")

    df = pd.read_csv(RAW_PATH)
    logging.info(f"Original shape: {df.shape}")

    df = df.dropna(subset=["Invoice", "Customer ID"])

    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")

    df = df[(df["Quantity"] > 0) & (df["Price"] > 0)]

    df["Description"] = df["Description"].fillna("Unknown Product")
    df["TotalAmount"] = df["Quantity"] * df["Price"]

    logging.info(f"Cleaned shape: {df.shape}")

    os.makedirs(os.path.dirname(PROCESSED_PATH), exist_ok=True)
    df.to_csv(PROCESSED_PATH, index=False)

    logging.info("Transformation completed")