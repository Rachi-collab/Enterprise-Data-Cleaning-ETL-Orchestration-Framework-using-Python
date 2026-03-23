import os
import logging

RAW_PATH = "/opt/airflow/data/raw/online_retail_II.csv"

def extract_data():
    logging.info("Extract stage started")

    if not os.path.exists(RAW_PATH):
        logging.error("Raw dataset not found")
        raise FileNotFoundError("Raw dataset missing")

    logging.info("Raw dataset found")